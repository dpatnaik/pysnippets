import dis

def greet(name):
	print('hello', name)

greet('abhinav')

dis.dis(greet)
