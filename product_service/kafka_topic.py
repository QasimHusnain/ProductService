#New Topic: In Kafka, a topic is a category or feed name to which records are sent. 
#A "new topic" refers to a topic that does not yet exist in the Kafka cluster but is being created.
#-------------------step 2 -  Kafka Topic Creation -------------------------------------
#Asynchronous functions handle the tasks that involve waiting for external resources without blocking the execution of other code
#aiokafka library used for administrative operations with Kafka, such as creating, deleting, or listing topics.
from aiokafka import NewTopic, settings
from aiokafka import AIOKafkaAdminClient
async def create_topic():                                                              #this function create a Kafka topic
    admin_client = AIOKafkaAdminClient(bootstrap_servers = settings.BOOTSTRAP_server)
#admin_client is an instance of the AIOKafkaAdminClient class - 
#The bootstrap_servers=settings.BOOTSTRAP_SERVER pass as parameter to AIOKafkaAdminClient class.
#bootstrap_servers is a configuration setting that tells the client where to find the Kafka cluster(brokers) 
#settings.BOOTSTRAP_SERVER is accessing the BOOTSTRAP_SERVER attribute from the settings module or object that is address of the Kafka server
#settings: This is a container for configuration values.
#BOOTSTRAP_SERVER: This is a specific configuration value (attribute) within settings
    await admin_client.start()#await: This keyword pause the execution of the asynchronous function until the (admin_client.start()) completes
#Start method establishes the connection to the Kafka cluster. This must be done before performing administrative tasks like creating a topic
    topic_list = [NewTopic(name = settings.KAFKA_ORDER_TOPIC, #Name of the topic to be created, which is stored in settings.KAFKA_ORDER_TOPIC.
                           num_partitions = 2,replication = 1)] #replication meaning only one replicas(copy)of the topic partitions.
#topic_list contains Kafka topic configurations
    try:   #Starts a try block to catch any exceptions that may occur during topic creation
       await admin_client.create_topics(new_topics = topic_list, validate_only = False) 
#await admin_client.create_topics sends a request to the Kafka broker to create the topics specified in topic_list.
#new_topics=topic_list: Passes the list of topics to be created
#validate_only=False: If set to True, the function will only validate the topic configuration without actually creating the topic. Here, it is set to False to ensure the topic is created.
       print(f"Topic '{settings.KAFKA_ORDER_TOPIC} created successfully" ) #If the topic creation is successful, this message is printed
    except Exception as e: #if an exception occurs,below line prints an error message along with the exception details
        print(f"failed to create topic'{settings.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        admin_client.close()






