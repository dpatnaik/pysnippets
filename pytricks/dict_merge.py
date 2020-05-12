# python 2.x
x = {'a':1, 'c':2}
y = {'c':3, 'd':4}
z = dict(x,**y)
print z

# python 3.5+
# x = {'a':1, 'c':2}
# y = {'c':3, 'd':4}
# z = {**x,**y}
# print z