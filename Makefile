help:
	@echo "Supported targets:"
	@echo "  test - run tests"
	@echo "  dist - generate distribution packages"
	@echo "  clean - remove generated files"

test:
	python -m unittest test_across

dist:
	python setup.py sdist bdist_wheel

clean:
	$(MAKE) -C doc clean
	rm -rf across.egg-info build dist

.PHONY: help test clean dist
