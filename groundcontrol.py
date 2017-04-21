from collections import deque
from select import select
from threading import Thread
from sockutils import bytes2long
from events import Event, ReceivedEvent


class GroundControl(Thread):
  """
  Listen for satellite messages on their sockets.
  """

  def __init__(self, sat_map, event_queue, signal, timeout=0.5):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Save the satellite dictionary to self.
    if not isinstance(sat_map.data, dict):
      raise TypeError('sat_map must be a locked dictionary')
    if not isinstance(event_queue.data, deque):
      raise TypeError('event_queue must be a deque')
    self._sat_map = sat_map
    self._gbl_queue = event_queue
    self._cond = signal
    self._timeout = timeout

  def run(self):
    while True:
      # Listen on the satellite sockets for events.
      sat_list = self._listen_for_events()
      # If the select was broken prematurely (e.g. OS event), start over.
      if not len(sat_list):
        continue
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
      with self._sat_map.lock:
        del self._sat_map.data[sat]
      return None
    event_len = bytes2long(hdr)
    # Receive the event.
    event_bytes = sat.recv(event_len)
    return Event().from_bytes(event_bytes)

  def _add_events_to_queue(self, events):
    if not len(events):
      return
    with self._gbl_queue.lock:
      self._gbl_queue.data.extendleft(events)
    with self._cond:
      self._cond.notify()
