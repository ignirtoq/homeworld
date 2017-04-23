from select import select
from socket import create_connection, gethostname, timeout, SHUT_RDWR
import threading
from time import sleep

from six import b

from core import default_core_port
from events import Event
from flag import Flag
from lockeddata import LockedData
from sockutils import long2bytes, bytes2long

# Unit test modules
import unittest as _ut


class NotConnectedError(RuntimeError):
  pass


class ConnectionError(RuntimeError):
  pass


class _SatCallback(object):
  """
  Shared received-event callback object.
  """

  def __init__(self, callback=None, *args, **kwargs):
    self.set_callback(callback, *args, **kwargs)

  def __call__(self, callback=None, *args, **kwargs):
    self.set_callback(callback, *args, **kwargs)

  def set_callback(self, callback=None, *args, **kwargs):
    self.callback = callback
    self.callback_args = args
    self.callback_kwargs = kwargs


class _SatListener(threading.Thread):
  """
  Event-receiver thread.
  """

  def __init__(self, socket, callback, event_list, terminate_flag, timeout=0.5):
    threading.Thread.__init__(self)
    self._socket = socket
    self._callback = callback
    self._event_list = event_list
    self._terminate_flag = terminate_flag
    self._timeout = timeout

  def run(self):
    while not self._terminate_flag:
      self._run_loop()

  def _run_loop(self):
    rd_list = select([self._socket], [], [], self._timeout)[0]
    if len(rd_list):
      event = self._get_event()
      self._process_event(event)

  def _get_event(self):
    msg = self._socket.recv(4)
    if not len(msg):
      # Message is zero-length, so the socket is closed.
      self._terminate_flag.set()
      return
    event_len = bytes2long(msg)
    event_bytes = self._socket.recv(event_len)
    return Event().from_bytes(event_bytes)

  def _process_event(self, event):
    """
    Passes a caught event to the callback function, if set.
    Otherwise appends event list stored in parent Satellite.
    """
    if self._callback.callback:
      callback = self._callback.callback
      args = self._callback.callback_args
      kwargs = self._callback.callback_kwargs
      callback(event, *args, **kwargs)
    else:
      with self._event_list.lock:
        self._event_list.data.append(event)


class Satellite(object):
  """
  Basic satellite for communication with a Core.
  """

  def __init__(self, timeout=2):
    self._timeout = timeout
    self._connected = False
    self._callback = _SatCallback()
    self._terminate_flag = Flag()
    self._events = LockedData([])
    self._event_types = []

  def launch(self, core_host=gethostname(), core_port=default_core_port):
    """
    Connect the the core.
    """
    core_addr = (core_host, core_port)
    try:
      self._socket = create_connection(core_addr, self._timeout)
    except timeout:
      raise ConnectionError('could not connect to Core')
    self._spawn_listener()
    self._connected = True

  def terminate(self):
    """
    Terminate the satellite's connection to the Core.

    To send and receive further events, the satellite will need to be relaunched
    with the launch() method.
    """
    self._check_connection()
    self._terminate_flag.set()
    sleep(0.75)
    self._socket.shutdown(SHUT_RDWR)
    self._socket.close()

  def event_callback(self, callback, *args, **kwargs):
    """
    Set the callback function executed when an event is received.

    This function must take the event as the first argument.  The remaining
    arguments provided to this function will be passed into the callback.
    """
    self._callback(callback, *args, **kwargs)

  def send_event(self, event):
    self._check_connection()
    event_bytes = event.to_bytes()
    event_len = long2bytes(len(event_bytes))
    self._socket.send(event_len)
    self._socket.send(event_bytes)

  def register(self, event_type):
    event = Event(type=b('register'), properties={b('type'): b(event_type)})
    self.send_event(event)
    self._event_types.append(event_type)

  def unregister(self, event_type):
    event = Event(type=b('unregister'), properties={b('type'): b(event_type)})
    self.send_event(event)
    self._event_types.remove(event_type)

  @property
  def events(self):
    retevents = []
    with self._events.lock:
      while len(self._events.data):
        retevents.append(self._events.data.pop())
    return retevents

  @property
  def connected(self):
    return self._connected

  @property
  def event_types(self):
    return [x for x in self._event_types]

  def _check_connection(self):
    if not self._connected:
      raise NotConnectedError('not connected to Core')

  def _spawn_listener(self):
    self._terminate_flag.unset()
    self._listener = _SatListener(socket=self._socket,
                                  event_list=self._events,
                                  callback=self._callback,
                                  terminate_flag=self._terminate_flag)
    self._listener.start()

  def _terminate_listener(self):
    self._terminate_flag.set()
    self._listener.join(timeout=1)
    return not self._listener.is_alive()
