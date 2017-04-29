from collections import deque
from select import select
from threading import Thread

from six import b

from sockutils import bytes2long
from events import Event, ReceivedEvent

# Unit test modules
import unittest as _ut
from threading import Condition as _Cond
from lockeddata import LockedData as _LD
from flag import Flag as _Flag
from sockutils import long2bytes as _l2b


class GroundControl(Thread):
  """
  Listen for satellite messages on their sockets.
  """

  def __init__(self, sat_map, event_sat_map, event_queue, signal,
               shutdown_flag, timeout=0.5):
    # Always call the parent Thread object's init function first.
    Thread.__init__(self)
    self._sat_map = sat_map
    self._event_sat_map = event_sat_map
    self._gbl_queue = event_queue
    self._cond = signal
    self._shutdown_flag = shutdown_flag
    self._timeout = timeout

  def run(self):
    while not self._shutdown_flag:
      self._run_loop()

  def _run_loop(self):
    # Listen on the satellite sockets for events.
    sat_list = self._listen_for_events()
    # If the select was broken prematurely (e.g. OS event), start over.
    if not len(sat_list):
      return
    # Loop over the sockets and receive their messages.
    event_queue = []
    for sat in sat_list:
      event = self._get_event(sat)
      if event:
        event_queue.append(ReceivedEvent(event, sat))
    self._add_events_to_queue(event_queue)

  def _listen_for_events(self):
    # Copy the currently registered satellite list.
    with self._sat_map.lock:
      rd_list = [x for x in self._sat_map.data]
    # Wait for a socket message.
    return select(rd_list,[],[], self._timeout)[0]

  def _get_event(self, sat):
    # Receive the header.
    hdr = sat.recv(4)
    # If the header is empty, the socket is closed, so remove the satellite
    # from the sat map.
    if not len(hdr):
      self._remove_sat(sat)
      return None
    event_len = bytes2long(hdr)
    # Receive the event.
    event_bytes = sat.recv(event_len)
    return Event().from_bytes(event_bytes)

  def _remove_sat(self, sat):
    # Remove satellites that have closed their connection from both the
    # satellite map and any event registration lists.
    with self._sat_map.lock:
      del self._sat_map.data[sat]
    with self._event_sat_map.lock:
      for ev_type in self._event_sat_map.data:
        try:
          self._event_sat_map.data[ev_type].remove(sat)
        except ValueError:
          continue

  def _add_events_to_queue(self, events):
    if not len(events):
      return
    with self._gbl_queue.lock:
      self._gbl_queue.data.extendleft(events)
    with self._cond:
      self._cond.notify()


class _GroundControlTestCase(_ut.TestCase):

  def setUp(self):
    self.ev = Event(type=b('test'))
    self.recv_call_count = 0
    class DummySat(object):
      def recv(sat_self, dummy):
        if self.recv_call_count % 2 == 0:
          self.recv_call_count += 1
          return _l2b(len(self.ev.to_bytes()))
        else:
          self.recv_call_count += 1
          return self.ev.to_bytes()
    self.sat = DummySat()
    global select
    def select(rd_list, wr_list, ex_list, timeout=None):
      return [self.sat],[],[]
    self.sat_map = _LD({self.sat: True})
    self.event_sat_map = _LD({b('all'): [self.sat]})
    self.event_queue = _LD(deque())
    self.signal = _Cond()
    self.flag = _Flag()
    self.gc = GroundControl(self.sat_map, self.event_sat_map, self.event_queue,
                            self.signal, self.flag)

  def test_add_ev_to_queue(self):
    self.assertEqual(len(self.event_queue.data), 0)
    self.gc._add_events_to_queue([self.ev])
    self.assertEqual(len(self.event_queue.data), 1)
    self.assertEqual(self.event_queue.data[0], self.ev)

  def test_get_event(self):
    ev = self.gc._get_event(self.sat)
    self.assertEqual(ev.to_bytes(), self.ev.to_bytes())

  def test_listen(self):
    rd_list = self.gc._listen_for_events()
    self.assertEqual(len(rd_list), 1)
    self.assertEqual(rd_list[0], self.sat)

  def test_remove_sat(self):
    self.assertTrue(self.sat in self.sat_map.data)
    self.assertTrue(self.sat in self.event_sat_map.data[b('all')])
    self.gc._remove_sat(self.sat)
    self.assertFalse(self.sat in self.sat_map.data)
    self.assertFalse(self.sat in self.event_sat_map.data[b('all')])

  def test_run_loop(self):
    self.assertEqual(len(self.event_queue.data), 0)
    self.gc._run_loop()
    self.assertEqual(len(self.event_queue.data), 1)
    self.gc._run_loop()
    self.assertEqual(len(self.event_queue.data), 2)
