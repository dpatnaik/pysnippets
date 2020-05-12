import json

my_mapping ={'a': 23, 'b': 46, 'c': 0xc0ffee}
print(my_mapping)
print(json.dumps(my_mapping, indent=4))

# Note this only works with dicts containing
# primitive types (check out the "pprint" module):
print(json.dumps({all:'yup'}))