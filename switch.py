from collections import deque
from threading import Thread
from sockutils import bytes2long
from six import b


class Switch(Thread):
  """
  Route events placed into its queue to registered satellites.
  """
  def __init__(self, queue=None, cond=None, sat_map=None, all_events_sats=None,
               event_sat_map=None):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Check that the variables are of the right types.
    if not isinstance(queue.data, deque):
      raise TypeError('queue must be a locked deque')
    if not isinstance(sat_map.data, dict):
      raise TypeError('sat_map must be a locked dictionary')
    if not isinstance(all_events_sats.data, list):
      raise TypeError('all_events_sats must be a locked list')
    if not isinstance(event_sat_map.data, dict):
      raise TypeError('event_sat_map must be a dict')
    self._gbl_queue = queue
    self._queue = deque()
    self._cond = cond
    self._sat_map = sat_map
    self._all_ev_sats = all_events_sats
    self._event_sat_map = event_sat_map

  def run(self):
    while True:
      with self._cond:
        while not len(self._gbl_queue):
          self._cond.wait()
        while len(self._gbl_queue):
          self._queue.appendleft(self._gbl_queue.pop())
      while len(self._queue):
        self._process_event(self._queue.pop())

  def _process_event(self, event):
    if event.type == b('Register'):
      self._process_registration(self, event)
    else:
      pass

  def _process_registration(self, event):
    # Drop registration events that don't have any properties.
    if not hasattr(event, 'properties'):
      return
    # Drop registration events without a "type" property.
    if b('type') not in event.properties:
      return
    if event.properties[b('type')].lower() == b('all'):
      # Add satellite to all-events list.
      pass
    else:
      # Add satellite to list for specified type.
      pass
