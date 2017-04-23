from collections import deque
from socket import socket, gethostname, SHUT_RDWR
from threading import Condition
from time import sleep

from six import b

from lockeddata import LockedData
from flag import Flag
from groundcontrol import GroundControl
from spaceport import Spaceport
from relay import Relay

# Unit test modules
import unittest as _ut

default_core_port = 51100


class InvalidCoreState(RuntimeError):
  pass


class CoreShutdownError(RuntimeError):
  pass


class Core(object):
  """
  Manages a home-automation satellite swarm.
  """
  def __init__(self, port=default_core_port, num_relays=4):
    self._init_data_structures(port, num_relays)

  def _init_data_structures(self, port, num_relays):
    self._clean = True
    self._num_relays = num_relays
    self._port = port
    # Map from socket to addr structure of each satellite.
    self._sat_map = LockedData(dict())
    # Map from event type to list of satellite sockets.
    self._event_sat_map = LockedData({b('all'): []})
    # Global queue of events to route.
    self._gbl_queue = LockedData(deque())
    # Condition variable for ground control to use to notify switches of new
    # events to route.
    self._cond = Condition()
    # Shutdown flag.  Signals child threads to shut down.
    self._shutdown_flag = Flag()

  def start(self):
    if not self._clean:
      raise InvalidCoreState('Core not cleanly shut down; cannot start')
    self._spawn_threads()

  def _spawn_threads(self):
    self._clean = False
    # Reset the shutdown flag in case Core is being restarted.
    self._shutdown_flag.unset()
    # Set up socket to listen for new satellites.
    self._public_sock = socket()
    self._public_sock.bind((gethostname(), self._port))
    self._public_sock.listen(1)
    # Construct and start the Spaceport.
    # This allows new satellites to connect to the Core.
    self._spaceport = Spaceport(socket=self._public_sock,
                                sat_map=self._sat_map,
                                shutdown_flag=self._shutdown_flag)
    self._spaceport.start()
    # Construct and start GroundControl.
    # This listens for events and passes them to the relays.
    self._gnd_control = GroundControl(sat_map=self._sat_map,
                                      event_sat_map=self._event_sat_map,
                                      event_queue=self._gbl_queue,
                                      signal=self._cond,
                                      shutdown_flag=self._shutdown_flag)
    self._gnd_control.start()
    # Construct and start the relays.
    # These register satellites to get or stop getting certain event types and
    # routes events caught by GroundControl to registered satellites.
    self._relays = [Relay(event_queue=self._gbl_queue,
                          signal=self._cond,
                          event_sat_map=self._event_sat_map,
                          shutdown_flag=self._shutdown_flag) \
                     for i in range(self._num_relays)]
    for relay in self._relays:
      relay.start()

  def _close_sockets(self, core, satellites):
    if core:
      self._public_sock.shutdown(SHUT_RDWR)
      self._public_sock.close()
    if satellites:
      for sat in self._sat_map.data:
        sat.shutdown(SHUT_RDWR)
        sat.close()

  def _join_thread(self, thread):
    thread.join(0.5)
    return not thread.is_alive()

  def _gen_shutdown_error_msg(self, sp_down, gc_down, r_down):
    err = 'Unable to shut down the following components: '
    needComma = False
    if not sp_down:
      err += ', Spaceport' if needComma else 'Spaceport'
      needComma = True
    if not gc_down:
      err += ', GroundControl' if needComma else 'GroundControl'
      needComma = True
    for i in range(len(r_down)):
      if not r_down[i]:
        if needComma: err += ', '
        err += 'Relay ' + str(i)
        needComma = True
    return err

  def shutdown(self):
    # Set the shutdown flag, wait a bit for threads to shutdown, then join them
    # with a timeout.  If any are still alive after the signal, raise an
    # exception.  If the shutdown wasn't "clean", don't allow a Core to be
    # restarted.
    self._shutdown_flag.set()
    # Notify the relays they need to wake up and shutdown.
    with self._cond:
      self._cond.notify_all()
    sleep(1)
    # Join the threads.
    spaceport_down = self._join_thread(self._spaceport)
    gnd_ctrl_down = self._join_thread(self._gnd_control)
    relay_down = [self._join_thread(relay) for relay in self._relays]
    # Close the sockets.
    # Close the public connection socket if the spaceport shutdown.
    # Close the satellite connections if ground control and all relays down.
    self._close_sockets(core=spaceport_down,
                        satellites=gnd_ctrl_down and all(relay_down))
    if spaceport_down and gnd_ctrl_down and all(relay_down):
      self._clean = True
    else:
      err = self._gen_shutdown_error_msg(spaceport_down, gnd_ctrl_down,
                                         relay_down)
      raise CoreShutdownError(err)


class _CoreTestCase(_ut.TestCase):
  def setUp(self):
    global socket
    global Spaceport, GroundControl, Relay
    self.sock_shutdown_count = 0
    class socket(object):
      def __init__(self, *args, **kwargs):
        pass
      def bind(self, *args, **kwargs):
        pass
      def listen(self, *args, **kwargs):
        pass
      def shutdown(sock_self, *args, **kwargs):
        self.sock_shutdown_count += 1
    class DummyThread(object):
      def __init__(self, *args, **kwargs):
        pass
      def start(self, *args, **kwargs):
        pass
      def join(self, *args, **kwargs):
        pass
      def is_alive(self, *args, **kwargs):
        return False
    Spaceport = DummyThread
    GroundControl = DummyThread
    Relay = DummyThread

  def test_core_restart(self):
    core = Core()
    core.start()
    core.shutdown()
    self.assertEqual(self.sock_shutdown_count, 1)
    core.start()

  def test_bad_restart(self):
    core = Core()
    core.start()
    with self.assertRaises(InvalidCoreState):
      core.start()
