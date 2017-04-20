import six
from six import next as it_next, iteritems as d_iteritems
from six import binary_type, byte2int, int2byte, iterbytes
from sockutils import long2bytes, bytes2long, iterbytes2long


class FormatError(Exception):
  pass


class Event(object):

  # Flags
  flag_recipient = 1 << 0
  flag_type = 1 << 1
  flag_properties = 1 << 2

  # Message version [major, minor]
  version = [0,1]

  def __init__(self, type=None, recipient=None, properties=None):
    self.type = type
    self.recipient = recipient
    self.properties = properties

  def to_bytes(self):
    # Version
    out  = int2byte(self.version[0])+int2byte(self.version[1])
    # Table of contents
    toc  = 0
    toc |= self.flag_recipient if self.recipient is not None else 0
    toc |= self.flag_type if self.type is not None else 0
    toc |= self.flag_properties if self.properties is not None else 0
    out += int2byte(toc)
    # Recipient, if there is one.
    # First size as a 32-bit int.
    if toc & self.flag_recipient:
      if not isinstance(self.recipient, binary_type):
        raise TypeError('Event recipient must be binary data')
      field_len = min(2**32-1, len(self.recipient))
      out += long2bytes(field_len)
      out += self.recipient[:field_len]
    if toc & self.flag_type:
      if not isinstance(self.type, binary_type):
        raise TypeError('Event type must be binary data')
      field_len = min(2**32-1, len(self.type))
      out += long2bytes(field_len)
      out += self.type[:field_len]
    if toc & self.flag_properties:
      if not isinstance(self.properties, dict):
        raise TypeError('properties must be a dictionary')
      # Encode the number of properties.
      num_prop = min(2**32-1, len(self.properties))
      out += long2bytes(num_prop)
      # Loop over the properties.
      i = 0
      for key, val in d_iteritems(self.properties):
        if type(key) is not six.binary_type:
          raise TypeError('property key must be binary data')
        if type(val) is not six.binary_type:
          raise TypeError('property value must be binary data')
        key_len = min(2**32-1, len(key))
        out += long2bytes(key_len)
        out += key[:key_len]
        val_len = min(2**32-1, len(val))
        out += long2bytes(val_len)
        out += val[:val_len]
    return out

  def from_bytes(self, mybytes):
    if len(mybytes) < 3:
      raise FormatError('input byte stream too short')
    it = iterbytes(mybytes)
    # Version
    self.version = [it_next(it), it_next(it)]
    # Table of contents
    toc = it_next(it)
    # Recipient field
    if toc & self.flag_recipient:
      field_len = iterbytes2long(it)
      self.recipient = binary_type()
      for i in range(field_len):
        self.recipient += int2byte(it_next(it))
    # Type field
    if toc & self.flag_type:
      field_len = iterbytes2long(it)
      self.type = binary_type()
      for i in range(field_len):
        self.type += int2byte(it_next(it))
    # Properties
    if toc & self.flag_properties:
      num_prop = iterbytes2long(it)
      self.properties = dict()
      for i in range(num_prop):
        key = binary_type()
        key_len = iterbytes2long(it)
        for j in range(key_len):
          key += int2byte(it_next(it))
        val = binary_type()
        val_len = iterbytes2long(it)
        for j in range(val_len):
          val += int2byte(it_next(it))
        self.properties[key] = val
    return self
