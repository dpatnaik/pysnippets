# useful in type checking tests using mypy
# pip install mypy-lang
# mypy type_annotation.py

def add_this(a: int, b: int) -> int:
	return a + b

print(add_this(4, 3))
print(add_this('a', ' b'))