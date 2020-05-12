# Different ways to test multiple
# flags at once in Python

x, y, z = 0, 1, 0
# x, y, z = 0, 100, 0

if x == 1 or y == 1 or z == 1:
	print("1 passed")

if 1 in (x, y, z):
	print("2 passed")

# These check for truthiness only
if x or y or z:
	print("3 passed")

if any((x, y, z)):
	print("4 passed")