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
    self.__socket = socket
    self.__callback = callback
    self.__event_list = event_list
    self.__terminate_flag = terminate_flag
    self.__timeout = timeout

  def run(self):
    while not self.__terminate_flag:
      self.__run_loop()

  def __run_loop(self):
    rd_list = select([self.__socket], [], [], self.__timeout)[0]
    if len(rd_list):
      event = self.__get_event()
      self.__process_event(event)

  def __get_event(self):
    msg = self.__socket.recv(4)
    if not len(msg):
      # Message is zero-length, so the socket is closed.
      self.__terminate_flag.set()
      return
    event_len = bytes2long(msg)
    event_bytes = self.__socket.recv(event_len)
    return Event().from_bytes(event_bytes)

  def __process_event(self, event):
    """
    Passes a caught event to the callback function, if set.
    Otherwise appends event list stored in parent Satellite.
    """
    if self.__callback.callback:
      callback = self.__callback.callback
      args = self.__callback.callback_args
      kwargs = self.__callback.callback_kwargs
      callback(event, *args, **kwargs)
    else:
      with self.__event_list.lock:
        self.__event_list.data.append(event)


class Satellite(object):
  """
  Basic satellite for communication with a Core.
  """

  def __init__(self, timeout=2):
    self.__timeout = timeout
    self.__connected = False
    self.__callback = _SatCallback()
    self.__terminate_flag = Flag()
    self.__events = LockedData([])
    self.__event_types = []

  def launch(self, core_host=gethostname(), core_port=default_core_port):
    """
    Connect the the core.
    """
    core_addr = (core_host, core_port)
    try:
      self.__socket = create_connection(core_addr, self.__timeout)
    except timeout:
      raise ConnectionError('could not connect to Core')
    self.__spawn_listener()
    self.__connected = True

  def terminate(self):
    """
    Terminate the satellite's connection to the Core.

    To send and receive further events, the satellite will need to be relaunched
    with the launch() method.
    """
    self.__check_connection()
    self.__terminate_flag.set()
    self.__listener.join(0.75)
    self.__socket.shutdown(SHUT_RDWR)
    self.__socket.close()

  def event_callback(self, callback, *args, **kwargs):
    """
    Set the callback function executed when an event is received.

    This function must take the event as the first argument.  The remaining
    arguments provided to this function will be passed into the callback.
    """
    self.__callback(callback, *args, **kwargs)

  def send_event(self, event):
    self.__check_connection()
    event_bytes = event.to_bytes()
    event_len = long2bytes(len(event_bytes))
    self.__socket.send(event_len)
    self.__socket.send(event_bytes)

  def register(self, event_type):
    event = Event(type=b('register'), properties={b('type'): b(event_type)})
    self.send_event(event)
    self.__event_types.append(event_type)

  def unregister(self, event_type):
    event = Event(type=b('unregister'), properties={b('type'): b(event_type)})
    self.send_event(event)
    self.__event_types.remove(event_type)

  @property
  def events(self):
    retevents = []
    with self.__events.lock:
      while len(self.__events.data):
        retevents.append(self.__events.data.pop())
    return retevents

  @property
  def connected(self):
    return self.__connected

  @property
  def event_types(self):
    return [x for x in self.__event_types]

  def __check_connection(self):
    if not self.__connected:
      raise NotConnectedError('not connected to Core')

  def __spawn_listener(self):
    self.__terminate_flag.unset()
    self.__listener = _SatListener(socket=self.__socket,
                                  event_list=self.__events,
                                  callback=self.__callback,
                                  terminate_flag=self.__terminate_flag)
    self.__listener.start()

  def __terminate_listener(self):
    self.__terminate_flag.set()
    self.__listener.join(timeout=1)
    return not self.__listener.is_alive()
