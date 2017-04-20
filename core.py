import socket
from spaceport import Spaceport


class Core(object):
  """
  Manages a home-automation satellite swarm.
  """
  def __init__(self, port=51100):
    self._public_sock = socket.socket()
    self._public_sock.bind((socket.gethostname(), port))
    self._sat_map = dict()
    # Construct and start the Spaceport.
    # This allows new satellites to connect to the Core.
    self._spaceport = Spaceport(sock=self._public_sock,
                                sat_map=self._sat_map)
    self._spaceport.start()
