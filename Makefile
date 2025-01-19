install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

lint:
	pylint --disable=R,C *.py &&\
	pylint --disable=R,C tests/*.py

test:
	python -m pytest -vv --cov=file_io tests/test_file_io.py
	python -m pytest -vv --cov=logger tests/test_logger.py
	python -m pytest -vv --cov=misc tests/test_misc.py

format:
	black *.py &&\
	black dqml_app/tests/*.py

all:
	install lint format test
