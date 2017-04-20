from threading import Thread


class Spaceport(Thread):
  """
  Establishes new connections on the Core's public socket
  """
  def __init__(self, sock=None, sat_map=None):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Save the public socket to self.
    if sock is None:
      raise TypeError('sock must be the Core's public socket object')
    self._sock = sock
    # Save the satellite dictionary to self.
    if not isinstance(sat_map, dict):
      raise TypeError('sat_map must be a dictionary')
    self._sat_map = sat_map

  def run(self):
    while True:
      # Accept new connections on the socket.
      (sat_sock, sat_addr) = accept()
      # Save connections to the satellite map.
      self._sat_map[sat_sock] = sat_addr
