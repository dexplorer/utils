install: requirements.txt 
	pip install --upgrade pip &&\
	pip install -r requirements.txt

# requirements.txt above is a dependency for install. \
It is supposed to run only if the requirements.txt has changed.

setup: 
	# python setup.py install
	pip install .

lint:
	pylint --disable=R,C *.py &&\
	pylint --disable=R,C utils/*.py &&\
	pylint --disable=R,C utils/tests/*.py

# pylint was failing in github actions CI pipeline. \
This is fixed by adding a setup target above. \
This ran ok in local env as setup is usually run during development. 

test:
	python -m pytest -vv --cov=utils utils/tests

# python -m finds the packages from parent directory. \
This is why pytest runs ok while pylint above fails without running setup first. \
This is a multi line comment.

format:
	black *.py &&\
	black utils/*.py
	black utils/tests/*.py

all: install setup lint format test 

# Make sure to specify the targets (install, setup, etc) in the same line as the 'all' target. \
These are dependencies. This is what makes them run in order.
