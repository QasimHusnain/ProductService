from aiokafka import AIOKafkaConsumer
#--------kafkaconsumer---
#A Kafka consumer subscribes to a Kafka topic to read (or consume) messages from it.





async def consume_messages(topic, bootstrap_servers):
#topic parameter that represents the name of the Kafka topic from which messages will be consumed.
#bootstrap_servers represents the address of the Kafka server(s) that the consumer will connect to. This is usually in the format host:port
    # Create a consumer instance.
    consumer = AIOKafkaConsumer( #variable that holds the instance of the AIOKafkaConsumer class
#AIOKafkaConsumer,a class from the aiokafka library
        topic, #this is passed as an argument to the AIOKafkaConsumer constructor. It tells the consumer which topic to listen to for messages.
        bootstrap_servers=bootstrap_servers, #This argument tells the consumer which Kafka server(s) to connect to.
        group_id="my-todos-group", #This sets the consumer group ID. A consumer group is a group of consumers that coordinate to read messages from a Kafka topic
        auto_offset_reset='earliest' #earliest' tells the consumer to start reading from the earliest available message in the topic. The alternative value could be 'latest', which would make the consumer start reading from the most recent message.  
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for messages in consumer: #try block: This is part of a try-except-finally structure in Python
#async for: This is a Python syntax for asynchronously iterating. messages: a variable that represents each message received from the Kafka topic.we can change the name as well.
            print(f"Received message: {messages.value.decode()} on topic {messages.topic}") #embedding expressions inside string literals using curly braces {}
            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally: #the code inside the finally block will always execute, regardless of whether an exception occurred in the try block.
        await consumer.stop()
