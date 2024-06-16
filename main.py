from fastapi import FastAPI, UploadFile
from fastapi.responses import HTMLResponse
import os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
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
    producer = AIOKafkaProducer(
        max_request_size=26214400, bootstrap_servers=address
    )
    await producer.start()

    print("transribing file")
    await producer.send_and_wait(
        "transcription.input",
        pickle.dumps(
            {
                "name": file.filename,
                "data": await file.read(),
            }
        ),
    )
    consumer = AIOKafkaConsumer(
        "transcription.output", bootstrap_servers=address
    )
    await consumer.start()
    message = await consumer.getone()
    await consumer.stop()
    text = message.value.decode()

    print("summarizing text")
    await producer.send_and_wait("summarization.input", str.encode(text))
    consumer = AIOKafkaConsumer(
        "summarization.output", bootstrap_servers=address
    )
    await consumer.start()
    message = await consumer.getone()
    await consumer.stop()
    summary = message.value.decode()

    await producer.stop()

    print("request completed")
    return {
        "text": text,
        "summary": summary,
    }
