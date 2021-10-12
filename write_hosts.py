import os
import sys


f = open("controller/config/hosts-list.txt", "a")
f.write(str(sys.argv))
f.close()
print("%s is written to a file ofl ist of hosts" % str(sys.argv))

