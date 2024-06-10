FROM python:3
WORKDIR /usr/src/app

RUN pip install aiokafka openai
COPY . .
CMD [ "python", "main.py" ]
