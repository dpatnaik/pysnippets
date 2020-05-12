# Sort a python dictionary by values
# (== get a representation of sorted by values)
import operator

x = {'a':4, 'b':3, 'c':2, 'd':1} 

print x
print x.items()
print sorted(x.items(), key=lambda x:x[1])
print sorted(x.items(), key=operator.itemgetter(1))

