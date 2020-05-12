# Python shorthand swap

a = 10
b = 5

# classic
tmp = a
a = b
b = tmp

print(a, b)

# Python

a, b = b, a

print(a, b)