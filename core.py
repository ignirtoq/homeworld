from collections import deque
import socket
from threading import Condition

from six import b

from lockeddata import LockedData
from groundcontrol import GroundControl
from spaceport import Spaceport


class Core(object):
  """
  Manages a home-automation satellite swarm.
  """
  def __init__(self, port=51100):
    self._init_data_structures()
    self._spawn_threads()

  def _init_data_structures(self):
    # Set up socket to listen for new satellites.
    self._public_sock = socket.socket()
    self._public_sock.bind((socket.gethostname(), port))
    # Map from socket to addr structure of each satellite.
    self._sat_map = LockedData(dict())
    # Map from event type to list of satellite sockets.
    self._event_sat_map = LockedData({b('all'): []})
    # Global queue of events to route.
    self._gbl_queue = LockedData(deque())
    # Condition variable for ground control to use to notify switches of new
    # events to route.
    self._cond = Condition()

  def _spawn_threads(self):
    # Construct and start the Spaceport.
    # This allows new satellites to connect to the Core.
    self._spaceport = Spaceport(sock=self._public_sock,
                                sat_map=self._sat_map)
    self._spaceport.start()
    # Construct and start GroundControl.
    # This listens for events and passes them to the switches.
    self._gnd_control = GroundControl(self._sat_map, self._gbl_queue,
                                      self._cond)
    self._gnd_control.start()
