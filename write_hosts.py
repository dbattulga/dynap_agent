import os
import sys


f = open("controller/config/hosts-list.txt", "a")
host = sys.argv[1]
f.write(host+"\n")
f.close()
print("%s is written to a file of list of hosts" % host)
