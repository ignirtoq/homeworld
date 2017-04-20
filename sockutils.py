from six import int2byte, iterbytes

def long2bytes(mylong):
  out = int2byte(mylong % 256)
  for i in range(3):
    mylong >>= 8
    out += int2byte(mylong % 256)
  return out

def iterbytes2long(it):
  out = 0
  i = 0
  for b in it:
    out += b << 8*i
    i += 1
    if i > 3:
      break
  return out

def bytes2long(mybytes):
  return iterbytes2long(iterbytes(mybytes))
