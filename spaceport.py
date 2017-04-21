from threading import Thread


class Spaceport(Thread):
  """
  Establishes new connections on the Core's public socket
  """
  def __init__(self, socket, sat_map):
    # Always call the parent Thread object's init function first.
    Thread.__init__()
    # Save the public socket to self.
    if socket is None:
      raise TypeError("sock must be the Core's public socket object")
    self._sock = socket
    # Save the satellite dictionary to self.
    if not isinstance(sat_map.data, dict):
      raise TypeError('sat_map must be a locked dictionary')
    self._sat_map = sat_map

  def run(self):
    while True:
      # Accept new connections on the socket.
      (sat_sock, sat_addr) = self._sock.accept()
      # Save connections to the satellite map.
      with self._sat_map.lock:
        self._sat_map.data[sat_sock] = sat_addr
