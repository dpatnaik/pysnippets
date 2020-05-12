import collections

c = collections.Counter(['hello', 'world'])
d = collections.Counter(['hello', 'earth'])
m = c.most_common(3)
print(c)
print(d)
print(m)