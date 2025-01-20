install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

lint:
	pylint --disable=R,C *.py &&\
	pylint --disable=R,C utils/*.py &&\
	pylint --disable=R,C utils/tests/*.py

test:
	python -m pytest -vv --cov=utils utils/tests

format:
	black *.py &&\
	black utils/*.py
	black utils/tests/*.py

all:
	install lint format test
