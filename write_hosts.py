import os
import sys


f = open("controller/config/hosts-list.txt", "a")
f.write(str(sys.argv))
f.close()
print(f"{sys.argv} is written to a file ofl ist of hosts")

