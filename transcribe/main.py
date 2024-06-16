import os
import asyncio
from openai import AsyncOpenAI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import pickle
import io

client = AsyncOpenAI()
address = os.environ.get("KAFKA_ADDRESS", "localhost:9092")


async def transcribe():
    producer = AIOKafkaProducer(
        max_request_size=26214400, bootstrap_servers=address
    )
    await producer.start()
    consumer = AIOKafkaConsumer(
        "transcription.input", bootstrap_servers=address
    )
    await consumer.start()

    while True:
        message = await consumer.getone()
        file = pickle.loads(message.value)

        text = ""

        CHUNK_SIZE = 26214400 - 1024
        for i in range(0, len(file["data"]), CHUNK_SIZE):
            audio_file = io.BytesIO(file["data"][i : i + CHUNK_SIZE])
            audio_file.name = file["name"]

            transcription = await client.audio.transcriptions.create(
                model="whisper-1", file=audio_file
            )
            text += transcription.text
        await producer.send_and_wait("transcription.output", str.encode(text))

    await producer.stop()
    await consumer.stop()

if __name__ == "__main__":
    asyncio.run(transcribe())
