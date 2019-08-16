help:
	@echo "Supported targets:"
	@echo "  test - run tests"
	@echo "  coverage - run tests and compute code coverage"
	@echo "  dist - generate distribution packages"
	@echo "  clean - remove generated files"

test:
	python -m unittest test_across

coverage:
	@# Generate .pth script for computing coverage in subprocesses.
	@# https://coverage.readthedocs.io/en/v4.5.x/subprocess.html
	echo 'import coverage;coverage.process_startup()' > \
	    `python -c 'import site;print(site.getsitepackages()[0])'`/across-coverage-startup.pth
	python -m coverage erase
	COVERAGE_FILE=`pwd`/.coverage COVERAGE_PROCESS_START=`pwd`/.coveragerc \
	    python -m coverage run -m unittest test_across
	@# We need to revert file path mangling applied by across importer (remove '*' appended to file names).
	@# This can potentially corrupt coverage files, so better keep your fingers crossed!
	sed -i 's/\.py\*/.py/g' .coverage.*
	python -m coverage combine

dist:
	python setup.py sdist bdist_wheel

clean:
	$(MAKE) -C doc clean
	rm -rf across.egg-info build dist .coverage.* .coverage htmlcov

.PHONY: help test coverage clean dist
