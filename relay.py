from collections import deque
from threading import Thread

from six import b

from sockutils import bytes2long, long2bytes

# Unit test modules
import unittest as _ut
import events as _ev
from flag import Flag as _Flag
from lockeddata import LockedData as _LD
from threading import Condition as _Cond

class Relay(Thread):
  """
  Route events placed into its queue to registered satellites.
  """
  def __init__(self, event_queue, signal, event_sat_map, shutdown_flag):
    # Always call the parent Thread object's init function first.
    Thread.__init__(self)
    self._gbl_queue = event_queue
    self._queue = deque()
    self._cond = signal
    self._event_sat_map = event_sat_map
    self._shutdown_flag = shutdown_flag

  def run(self):
    while not self._shutdown_flag:
      self._run_loop()

  def _run_loop(self):
    self._get_events()
    while len(self._queue):
      self._process_event(self._queue.pop())

  def _get_events(self):
    with self._cond:
      while not len(self._gbl_queue.data):
        self._cond.wait()
      while len(self._gbl_queue.data):
          self._queue.appendleft(self._gbl_queue.data.pop())

  def _process_event(self, rec_event):
    event = rec_event.event
    if event.type.lower() == b('register') \
    or event.type.lower() == b('unregister'):
      self._process_register_event(rec_event)
    else:
      self._route_event(rec_event)

  def _route_event(self, rec_event):
    event = rec_event.event
    sats_sent = {}
    for sat in self._event_sat_map.data[b('all')]:
      if sat not in sats_sent:
        _send_event(event, sat)
        sats_sent[sat] = True
    if event.type in self._event_sat_map.data:
      for sat in self._event_sat_map.data[event.type]:
        if sat not in sats_sent:
          _send_event(event, sat)
          sats_sent[sat] = True

  def _process_register_event(self, rec_event):
    event = rec_event.event
    sat = rec_event.source
    # Drop registration events that don't have any properties.
    if not hasattr(event, 'properties'):
      return
    # Drop registration events without a "type" property.
    if b('type') not in event.properties:
      return
    if event.type.lower() == b('register'):
      # Add satellite to list for specified type.
      self._add_sat_event(sat, event.properties[b('type')])
    elif event.type.lower() == b('unregister'):
      # Remove satellite from list for specified type.
      self._remove_sat_event(sat, event.properties[b('type')])

  def _add_sat_event(self, sat, ev_type):
    # Lock the event sat map to modify it.
    with self._event_sat_map.lock:
      ev_sat_map = self._event_sat_map.data
      # Create a new empty list if no satellites have registered for this type.
      if ev_type not in ev_sat_map:
        ev_sat_map[ev_type] = list()
      # Add the satellite if it's not already in the list.
      try:
        ev_sat_map[ev_type].index(sat)
      except ValueError:
        ev_sat_map[ev_type].append(sat)

  def _remove_sat_event(self, sat, ev_type):
    # Lock the event sat map to modify it.
    with self._event_sat_map.lock:
      ev_sat_map = self._event_sat_map.data
      # Bail if no list for the event type exists.
      if ev_type not in ev_sat_map:
        return
      # Add the satellite if it's not already in the list.
      try:
        ev_sat_map[ev_type].remove(sat)
      except ValueError:
        pass


def _send_event(event, sat):
  event_bytes = event.to_bytes()
  event_len = long2bytes(len(event_bytes))
  sat.send(event_len)
  sat.send(event_bytes)


class _RelayTestCase(_ut.TestCase):

  def setUp(self):
    # Set up a dummy satellite object that counts calls to send.
    self.sat_send_called = 0
    class DummySat(object):
      def send(sat_self, data):
        self.sat_send_called += 1
    self.sat = DummySat()
    self.queue = _LD(deque())
    self.signal = _Cond()
    self.ev_sat_map = _LD({b('all'): [self.sat]})
    self.flag = _Flag()
    self.relay = Relay(self.queue, self.signal, self.ev_sat_map, self.flag)

  def test_send_all(self):
    # Create event to process.
    ev = _ev.Event(type=b('test'))
    rec_ev = _ev.ReceivedEvent(ev, self.sat)
    self.relay._process_event(rec_ev)
    self.assertEqual(self.sat_send_called, 2)

  def test_add_sat(self):
    self.assertFalse(b('test') in self.ev_sat_map.data)
    self.relay._add_sat_event(self.sat, b('test'))
    self.assertTrue(b('test') in self.ev_sat_map.data)

  def test_remove_sat(self):
    self.ev_sat_map.data[b('test')] = [self.sat]
    self.assertTrue(self.sat in self.ev_sat_map.data[b('test')])
    self.relay._remove_sat_event(self.sat, b('test'))
    self.assertFalse(self.sat in self.ev_sat_map.data[b('test')])

  def test_register_event(self):
    # Create register event to process.
    ev = _ev.Event(type=b('register'),
                   properties={b('type'): b('test')})
    rec_ev = _ev.ReceivedEvent(ev, self.sat)
    self.assertFalse(not hasattr(ev, 'properties'))
    self.assertFalse(b('type') not in ev.properties)
    self.assertFalse(b('test') in self.ev_sat_map.data)
    self.relay._process_register_event(rec_ev)
    self.assertEqual(self.sat_send_called, 0)
    self.assertTrue(b('test') in self.ev_sat_map.data)

  def test_register(self):
    # Create register event to process.
    ev = _ev.Event(type=b('register'),
                   properties={b('type'): b('test')})
    rec_ev = _ev.ReceivedEvent(ev, self.sat)
    self.assertFalse(b('test') in self.ev_sat_map.data)
    self.relay._process_event(rec_ev)
    self.assertEqual(self.sat_send_called, 0)
    self.assertTrue(b('test') in self.ev_sat_map.data)

  def test_relay(self):
    # Add sat to b'test' types.
    self.ev_sat_map.data[b('test')] = [self.sat]
    # Create test event to relay.
    ev = _ev.Event(type=b('test'),
                   properties={b('type'): b('test')})
    rec_ev = _ev.ReceivedEvent(ev, self.sat)
    self.assertEqual(self.sat_send_called, 0)
    self.relay._process_event(rec_ev)
    self.assertEqual(self.sat_send_called, 2)

  def test_run_loop(self):
    # Create test event and add to queue.
    ev = _ev.Event(type=b('test'),
                   properties={b('type'): b('test')})
    rec_ev = _ev.ReceivedEvent(ev, self.sat)
    self.queue.data.append(rec_ev)
    self.assertEqual(self.sat_send_called, 0)
    self.relay._run_loop()
    self.assertEqual(self.sat_send_called, 2)
