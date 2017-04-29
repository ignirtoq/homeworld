import unittest as _ut

class Flag(object):

  def __init__(self):
    self.value = False

  def __nonzero__(self):
    return self.value

  def __bool__(self):
    return self.__nonzero__()

  def set(self):
    self.value = True

  def unset(self):
    self.value = False


class _FlagTestCase(_ut.TestCase):

  def setUp(self):
    self.flag = Flag()

  def test_default(self):
    self.assertFalse(self.flag)

  def test_set(self):
    self.flag.set()
    self.assertTrue(self.flag)

  def test_unset(self):
    self.flag.set()
    self.assertTrue(self.flag)
    self.flag.unset()
    self.assertFalse(self.flag)
