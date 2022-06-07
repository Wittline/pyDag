FROM python:3.8

WORKDIR /pydag

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN pip install importlib

COPY ./app ./app

CMD ["python", "-u", "./app/test.py"]