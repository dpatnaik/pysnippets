 # when you’re sorting items in a list, be sure to use keys and the default sort() method whenever possible. 
 # In the following, 
 # notice that in each case the list is sorted according to the index you select as part of the key argument.
 # This approach works just as well with strings as it does with numbers
import operator

somelist = [(1, 5, 8), (6, 2, 4), (9, 7, 5)]

somelist.sort(key=operator.itemgetter(0))
print(somelist)
#Output = [(1, 5, 8), (6, 2, 4), (9, 7, 5)]

somelist.sort(key=operator.itemgetter(1))
print(somelist)
#Output = [(6, 2, 4), (1, 5, 8), (9, 7, 5)]

somelist.sort(key=operator.itemgetter(2))
print(somelist)
#Output = [(6, 2, 4), (9, 7, 5), (1, 5, 8)],