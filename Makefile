help:
	@echo "Supported targets: test, clean"

test:
	python -m unittest test_across

clean:
	$(MAKE) -C doc clean

.PHONY: help test clean
