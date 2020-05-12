from collections import namedtuple
import json

def _json_object_hook(d):
	return namedtuple('X', d.keys())(*d.values())

def json2obj(data):
	return json.loads(data, object_hook=_json_object_hook)


data = '{"name": "John Smith", "hometown": {"name": "New York", "id": 123}}'
x = json2obj(data)
print(x)
print(x.name)
print(x.hometown)
print(x.hometown.name)