# Local

APP := utils

install-dev: pyproject.toml
	pip install --upgrade pip &&\
	pip install --editable .[all-dev]
	# TMPDIR=/home/ec2-user/pip_cache pip install --cache-dir=/home/ec2-user/pip_cache --editable .[spark] &&\

# pyproject.toml above is a dependency for install. 
# It is supposed to run only if pyproject.toml has changed.

lint:
	pylint --disable=R,C src/${APP}/*.py &&\
	pylint --disable=R,C tests/*.py

# pylint was failing in github actions CI pipeline. 
# This is fixed by adding a setup target above. 
# This ran ok in local env as setup is usually run during development. 

test:
	python -m pytest -vv --cov=src/${APP} tests

# python -m finds the packages from parent directory. \
This is why pytest runs ok while pylint above fails without running setup first. \
This is a multi line comment.

format:
	black src/${APP}/*.py &&\
	black tests/*.py

	isort src/${APP}/*.py &&\
	isort tests/*.py

local-all: install-dev lint format test 

# Make sure to specify the targets (install, setup, etc) in the same line as the 'all' target. \
These are dependencies. This is what makes them run in order.
