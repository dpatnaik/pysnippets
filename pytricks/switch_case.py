def dispatch_if(operator, a, b):
	if operator == 'add':
		return a + b
	elif operator == 'sub':
		return a - b
	elif operator == 'mul':
		return a * b
	elif operator == 'div':
		return a / b
	else:
		return None


def dispatch_dict(operator, a, b):
	return {
		'add' : lambda: a + b,
		'sub' : lambda: a - b,
		'mul' : lambda: a * b,
		'div' : lambda: a / b,
		}.get(operator, lambda: None)()


print(dispatch_if('div', 8, 7))
print(dispatch_dict('dk', 8, 7))
