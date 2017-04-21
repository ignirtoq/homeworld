from threading import Lock


class LockedData(object):
  def __init__(self, data, lock=Lock()):
    self.data = data
    self.lock = lock
