lst = [1,2,3,4,5]
# clear elements from a list
del lst[:]
print(lst)

a = lst
print(a)
print(lst)
# replace all elements of a list without creating new object
lst[:] = [7,8,9]
print(a)
print(lst)

# create a (shallow) copy of a list
b = lst[:]
print(b)

print(a is lst)
print(a == lst)
print(b is lst)
print(b == lst)