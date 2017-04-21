from collections import deque
from threading import Thread
from sockutils import bytes2long, long2bytes
from six import b


class Relay(Thread):
  """
  Route events placed into its queue to registered satellites.
  """
  def __init__(self, queue, signal, sat_map, event_sat_map):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Check that the variables are of the right types.
    if not isinstance(queue.data, deque):
      raise TypeError('queue must be a locked deque')
    if not isinstance(sat_map.data, dict):
      raise TypeError('sat_map must be a locked dictionary')
    if not isinstance(event_sat_map.data, dict):
      raise TypeError('event_sat_map must be a dict')
    self._gbl_queue = queue
    self._queue = deque()
    self._cond = signal
    self._sat_map = sat_map
    self._event_sat_map = event_sat_map

  def run(self):
    while True:
      self._get_events()
      while len(self._queue):
        self._process_event(self._queue.pop())

  def _get_events(self):
    with self._cond:
      while not len(self._gbl_queue):
        self._cond.wait()
      while len(self._gbl_queue):
          self._queue.appendleft(self._gbl_queue.pop())

  def _process_event(self, rec_event):
    event = rec_event.event
    if event.type.lower() == b('register')
    or event.type.lower() == b('unregister'):
      self._process_register_event(rec_event)
    else:
      self._route_event(rec_event)

  def _route_event(self, rec_event):
    event = rec_event.event
    for sat in self._event_sat_map[b('all')]:
      _send_event(event, sat)
    if event.type in self._event_sat_map:
      for sat in self._event_sat_map[event.type]:
        _send_event(event, sat)

  def _process_register_event(self, rec_event):
    event = rec_event.event
    sat = rec_event.source
    # Drop registration events that don't have any properties.
    if not hasattr(event, 'properties'):
      return
    # Drop registration events without a "type" property.
    if b('type') not in event.properties:
      return
    if event.properties[b('type').lower()] == b('register'):
      # Add satellite to list for specified type.
      self._add_sat_event(sat, event.properties[b('type')])
    elif event.properties[b('type').lower()] == b('register'):
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
