.PHONY: clean test flake8

clean:
	find . -type f -name "*.py[co]" -delete

flake8:
	flake8 .

test: clean flake8
