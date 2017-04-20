from select import select
from threading import Thread
from sockutils import bytes2long


class GroundControl(Thread):
  """
  Listen for satellite messages on their sockets.
  """
  def __init__(self, sat_map=None):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Save the satellite dictionary to self.
    if not isinstance(sat_map, dict):
      raise TypeError('sat_map must be a dictionary')
    self._sat_map = sat_map

  def run(self):
    while True:
      rd_list = select([x for x in self._sat_map],[],[])[0]
      if not len(rd_list):
        continue
