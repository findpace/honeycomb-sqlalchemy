static:
	pre-commit run --all-files

test:
	py.test -v

coverage:
	coverage run -m pytest test -v
	coverage report