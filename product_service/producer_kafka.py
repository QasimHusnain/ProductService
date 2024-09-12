from aiokafka import AIOKafkaProducer

# Kafka Producer as a dependency
async def get_kafka_producer(): #async function with no parameters
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')  #producer variable that is an instance of AIOkafkaProducer and bootstrap_servers as parameter
    await producer.start() #await untill the producer start get complete
    try:
        yield producer #yield used to lazzy the producer function
    finally:
        await producer.stop() #funally close the resources even try block failed to run


