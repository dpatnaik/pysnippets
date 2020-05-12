# Using namedtuple is way shorter than
# defining a class manually:

from collections import namedtuple

Car = namedtuple('Car', 'color mileage')

my_car = Car('red', 23.4)

# We get a nice string repr for free:
print(my_car)

print(my_car.color)

# Like tuples, namedtuples are immutable:
# my_car.color = 'blue'