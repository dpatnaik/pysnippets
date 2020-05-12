# Function arguent unpacking

def my_func(x, y, c):
	print(x, y, c)

tuple1 = (1,0,-2)
dict1 = {'x':1, 'y':0, 'c':-1} #key names must match arguments

my_func(*tuple1)
my_func(**dict1)
