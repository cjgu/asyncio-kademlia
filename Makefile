.PHONY: clean test flake8

clean:
	find . -type f -name "*.py[co]" -delete

flake8:
	flake8 .

test: clean flake8

node1:
	python node.py 10000

node2:
	python node.py 10001 --remote=127.0.0.1:10000
