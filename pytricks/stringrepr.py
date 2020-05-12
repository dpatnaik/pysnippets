import datetime

today = datetime.date.today()

print(today)
# Result of __str__ should be readable:
print(str(today))
# Result of __repr__ should be unambiguous:
print(repr(today))

# Python interpreter sessions use 
# __repr__ to inspect objects:
today