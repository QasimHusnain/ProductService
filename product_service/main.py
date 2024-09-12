from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated
from . import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select, Sequence
from fastapi import FastAPI, Depends
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import logging
from . import consumer_kafka







# connection_string = str(settings.DATABASE_URL).replace(
#     "postgresql", "postgresql+psycopg"
# )


# # recycle connections after 5 minutes
# # to correspond with the compute scale down
# engine = create_engine(
#     connection_string, connect_args={}, pool_recycle=300
# )

# #engine = create_engine(
# #    connection_string, connect_args={"sslmode": "require"}, pool_recycle=300
# #)


# def create_db_and_tables()->None:
#     SQLModel.metadata.create_all(engine)

# def get_session():
#     with Session(engine) as session:
#       yield session



# async def consume_messages(topic, bootstrap_servers):
#     # Create a consumer instance.
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="my-todos-group",
#         auto_offset_reset='earliest'    
#     )

#     # Start the consumer.
#     await consumer.start()
#     try:
#         # Continuously listen for messages.
#         async for message in consumer:
#             print(f"Received message: {message.value.decode()} on topic {message.topic}")
#             # Here you can add code to process each message.
#             # Example: parse the message, store it in a database, etc.
#     finally:
#         # Ensure to close the consumer when done.
#         await consumer.stop()


# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
# loop = asyncio.get_event_loop()
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")
    # loop.run_until_complete(consume_messages('todos', 'broker:19092'))
    task = asyncio.create_task(consumer_kafka.consume_messages('todos', 'broker:19092'))
    consumer_kafka.create_db_and_tables()
    yield

 

app = FastAPI(lifespan=lifespan, title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "http://127.0.0.1:8002", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://127.0.0.1:8000", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])



# Kafka Producer as a dependency
# async def get_kafka_producer():
#     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#     await producer.start()
#     try:
#         yield producer
#     finally:
#         await producer.stop()

# @app.get("/")
# def read_root():
#     return {"App": "Service 2"}

#--------------------------------------------------------------
# connection_string=str(settings.DATABASE_URL)
# engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

# def create_tables():
#     SQLModel.metadata.create_all(engine)


# def get_session():
#     with Session(engine) as session:
#         yield session


# async def create_topic():
#     admin_client = AIOKafkaAdminClient(
#         bootstrap_servers=settings.BOOTSTRAP_SERVER)
#     await admin_client.start()
#     topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
#                            num_partitions=2, replication_factor=1)]
#     try:
#         await admin_client.create_topics(new_topics=topic_list, validate_only=False)
#         print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
#     except Exception as e:
#         print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
#     finally:
#         await admin_client.close()

# async def consume_messages():
#     # Create a consumer instance.
#     consumer = AIOKafkaConsumer(
#         settings.KAFKA_ORDER_TOPIC,
#         bootstrap_servers=settings.BOOTSTRAP_SERVER,
#         group_id=settings.KAFKA_CONSUMER_GROUP_ID,
#         auto_offset_reset='earliest',
#         # session_timeout_ms=30000,  # Example: Increase to 30 seconds
#         # max_poll_records=10,
#     )
#     # Start the consumer.
#     await consumer.start()
#     try:
#         with Session(engine) as session:
#         # Continuously listen for messages.
#             async for msg in consumer:
#                 if msg.value is not None:
#                     try:
#                         product = product_pb2.product()
#                         product.ParseFromString(msg.value)            
#                         data_from_producer=Products(id=product.id, name= product.name, price=product.price, quantity = product.quantity)
                        
#                         if product.type == product_pb2.Operation.CREATE:
#                                 session.add(data_from_producer)
#                                 session.commit()
#                                 session.refresh(data_from_producer)
#                                 print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

#                         elif product.type== product_pb2.Operation.DELETE:
#                                 product_to_delete = session.get(Products, product.id)
#                                 if product_to_delete:
#                                     session.delete(product_to_delete)
#                                     session.commit()
#                                     logging.info(f"Deleted product with ID: {product.id}")
#                                 else:
#                                     logging.warning(f"Product with ID {product.id} not found for deletion")
#                         elif product.type== product_pb2.Operation.PUT:
#                                 db_product = session.get(Products, product.id)
#                                 if db_product:
#                                     db_product.id = product.id
#                                     db_product.name = product.name
#                                     db_product.price = product.price
#                                     session.add(db_product)
#                                     session.commit()
#                                     session.refresh(db_product)
#                                     logging.info(f"Updated product with ID: {product.id}")
#                                 else:
#                                     logging.warning(f"Product with ID {product.id} not found for update")

#                     except:
#                         logging.error(f"Error deserializing messages")
#                 else :
#                     print("Msg has no value")
#     except Exception as e: 
#         logging.error(f"Error consuming messages: {e}")
#     finally:
#         await producer.stop()

# app = FastAPI(lifespan=lifespan,
#               title="Zia Mart User Service...",
#               version='1.0.0'
#               )


# @app.get('/')
# async def root():
#    return{"welcome to zia mart","product_service"}


# #---------------------My own code for product-service-------------------

# ###------------- creation of database -----------------
# #step 1 - create a variable , add configuration setting that contains the URL for connecting to your database.
# #.replace  This method call replaces the substring "postgresql" with "postgresql+psycopg" in the connection string
# #because SQLAlchemy uses the psycopg dialect to interface with PostgreSQL databases, and this code adjusts the URL format accordingly.
# connect_string = str(settings.DATABASE_URL).replace("postgresql","postgresql+psycopg")
# #step 2 - create a SQLAlchemy engine -make a variable engine.
# #it's a core component that manages the connection to the database that provides the interface to execute SQL queries and manage transactions
# #create_engine is a predefine function, imported from sqlmodel - connect_string, that specifies how to connect to the database
# #extra_args ={} empty dictionary allows to pass additional arguments.for example,configure connection-specific settings like SSL options or timeout settings.
# #might pass {"sslmode": "require"}
# #pool_recycle parameter sets the interval (in seconds) after which database connections in the connection pool are recycled (i.e., closed and reopened)
# engine = create_engine(connect_string, pool_recycle =300, extra_args ={})
# #step 3 - create a function that create database and tables scheme.it does'nt return any value.
# #Function Body - SQLModel, a class from the sqlmodel library, define database models and schemas.It provides a way to interact with SQL databases
# #metadata - An attribute of the SQLModel class.It holds metadata about all the models defined that includes information about the tables, columns, and their relationships.
# #create_all -This is a method of the metadata object that generates SQL commands to create all tables defined by the models. 
# #engine - It is an argument uses to execute the commands. This function is crucial for setting up the initial state of the database
# def create_db_tables_schema() -> None:
#     SQLModel.metedata.create_all(engine)
# #step 4 -getting_session function is responsible for providing or "getting" a session object.
# #Session refers a class from SQLAlchemy that manage database transactions.It is a class that creates a session object.
# #with statement ensures that resources are properly managed.Once the block of code under the with statement completes, the session is automatically closed, even if an error occurs.
# def getting_session(): #No parameters
#     with Session(engine) as session: #engine passed to the Session class to connect to the database.
#         yield session #yield keyword is used in a generator function. It allows the function to return a value and pause its execution and resumed later.In this case, yield is used to produce the session object.
# #Instead of returning the session object and exiting the function, yield provides the session object to the caller and allows the function to maintain its state. This is particularly useful in dependency injection scenarios where the caller of get_session might want to use the session for a while and then discard it when done.
# #yield keyword turn the get_session function into a generator that allows the session to be provided to the caller in a lazy manner.
# #Generator function - is defined with def but instead of using return to single value, it uses the yield keyword to yield multiple values one at a time
# #Generators compute values on-the-fly as you iterate over them, rather than computing all values at once and storing them in memory