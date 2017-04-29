from select import select
from threading import Thread

# Unit test modules
import unittest as _ut
from lockeddata import LockedData as _LD
from flag import Flag as _Flag


class Spaceport(Thread):
  """
  Establishes new connections on the Core's public socket
  """
  def __init__(self, socket, sat_map, shutdown_flag, timeout=0.5):
    # Always call the parent Thread object's init function first.
    Thread.__init__(self)
    self._sock = socket
    self._sat_map = sat_map
    self._shutdown_flag = shutdown_flag
    self._timeout = timeout

  def run(self):
    while not self._shutdown_flag:
      self._run_loop()

  def _run_loop(self):
    rd_list = select([self._sock],[],[], self._timeout)[0]
    if self._sock in rd_list:
      self._accept_new_connection()

  def _accept_new_connection(self):
    # Accept new connections on the socket.
    (sat_sock, sat_addr) = self._sock.accept()
    # Save connections to the satellite map.
    with self._sat_map.lock:
      self._sat_map.data[sat_sock] = sat_addr


class _SpaceportTestCase(_ut.TestCase):

  def setUp(self):
    global select
    class DummySocket(object):
      def accept(self):
        return (1, 1)
    self.sock = DummySocket()
    def select(rd_list, wr_list, ex_list, timeout=None):
      return [self.sock], [], []
    self.sat_map = _LD(dict())
    self.flag = _Flag()
    self.spaceport = Spaceport(self.sock, self.sat_map, self.flag)

  def test_run_loop(self):
    self.assertEqual(len(self.sat_map.data), 0)
    self.spaceport._run_loop()
    self.assertEqual(len(self.sat_map.data), 1)
    self.assertTrue(1 in self.sat_map.data)
    self.assertEqual(self.sat_map.data[1], 1)
