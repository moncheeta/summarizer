FROM python:3
WORKDIR /usr/src/app

RUN pip install fastapi aiokafka
COPY . .
EXPOSE 8000
CMD [ "fastapi", "run" ]
