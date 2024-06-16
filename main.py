from fastapi import FastAPI, UploadFile
from fastapi.responses import HTMLResponse
import os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from uuid import uuid4 as uuid
import pickle

app = FastAPI()
address = os.environ.get("KAFKA_ADDRESS", "localhost:9092")


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(
        content="""
    <html>
        <form enctype="multipart/form-data" method="post">
            <input type="file" name="file" />
            <button type="submit">Submit</button>
        </form>
    </html>
    """
    )


@app.post("/")
async def summarize(file: UploadFile):
    job_id = str.encode(str(uuid()))

    producer = AIOKafkaProducer(
        max_request_size=26214400, bootstrap_servers=address
    )
    await producer.start()
    print("transribing file")
    consumer = AIOKafkaConsumer(
        "transcription.output", bootstrap_servers=address
    )
    await consumer.start()
    await producer.send_and_wait(
        "transcription.input",
        key=job_id,
        value=pickle.dumps(
            {
                "name": file.filename,
                "data": await file.read(),
            }
        ),
    )
    text = ""
    while True:
        message = await consumer.getone()
        if message.key == job_id:
            text = message.value.decode()
            break
    await consumer.stop()

    print("summarizing text")
    consumer = AIOKafkaConsumer(
        "summarization.output", bootstrap_servers=address
    )
    await consumer.start()
    await producer.send_and_wait(
        "summarization.input", key=job_id, value=str.encode(text)
    )

    summary = ""
    while True:
        message = await consumer.getone()
        if message.key == job_id:
            summary = message.value.decode()
            break
    await consumer.stop()

    await producer.stop()

    print("request completed")
    return {
        "text": text,
        "summary": summary,
    }
