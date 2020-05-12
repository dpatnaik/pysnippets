# vals = [expression for val in collection if condition]
# 
# instead of 
# 
# vals = []
# for val in collection:
# 	if condition:
#		vals.append(expression)

even_squares = [x * x for x in range(10) if x % 2 == 0]

print(even_squares)