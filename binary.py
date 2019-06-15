import struct

i = 10

str = '123'

fo = 1.5

f = open('bin.bin', 'wb')

bt = struct.pack('3s', bytes(str, 'ascii'))

print(bt)

f.write(bt)
