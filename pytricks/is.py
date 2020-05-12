a = [1, 2, 3]

b = a

print(b is a)

print(a == b)

c = list(a)

print(c is a)

print(a == c)

# "is" expressions evaluate to true if two variables point to the same object
# == evaluates to true if the objects referred to by the variables are equal