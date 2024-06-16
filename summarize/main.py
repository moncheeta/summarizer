import os
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from openai import AsyncOpenAI

client = AsyncOpenAI()
address = os.environ.get("KAFKA_ADDRESS", "localhost:9092")


async def summarizer():
    producer = AIOKafkaProducer(bootstrap_servers=address)
    await producer.start()
    consumer = AIOKafkaConsumer(
        "summarization.input", bootstrap_servers=address
    )
    await consumer.start()

    while True:
        message = await consumer.getone()
        text = message.value.decode()

        summarization = await client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": "summarize the following:\n" + text,
                }
            ],
            model="gpt-4-turbo",
        )
        summary = summarization.choices[0].message.content
        await producer.send_and_wait("summarization.output", str.encode(summary))

    await producer.stop()
    await consumer.stop()


if __name__ == "__main__":
    asyncio.run(summarizer())
