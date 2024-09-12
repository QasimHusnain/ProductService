from . import settings
from sqlmodel import SQLModel, create_engine,Session


###------------- creation of database -----------------
#step 1 - create a variable , add configuration setting that contains the URL for connecting to your database.
#.replace  This method call replaces the substring "postgresql" with "postgresql+psycopg" in the connection string
#because SQLAlchemy uses the psycopg dialect to interface with PostgreSQL databases, and this code adjusts the URL format accordingly.
connect_string = str(settings.DATABASE_URL).replace("postgresql","postgresql+psycopg")
#step 2 - create a SQLAlchemy engine -make a variable engine.
#it's a core component that manages the connection to the database that provides the interface to execute SQL queries and manage transactions
#create_engine is a predefine function, imported from sqlmodel - connect_string, that specifies how to connect to the database
#extra_args ={} empty dictionary allows to pass additional arguments.for example,configure connection-specific settings like SSL options or timeout settings.
#might pass {"sslmode": "require"}
#pool_recycle parameter sets the interval (in seconds) after which database connections in the connection pool are recycled (i.e., closed and reopened)
engine = create_engine(connect_string, pool_recycle =300, extra_args ={})
#step 3 - create a function that create database and tables scheme.it does'nt return any value.
#Function Body - SQLModel, a class from the sqlmodel library, define database models and schemas.It provides a way to interact with SQL databases
#metadata - An attribute of the SQLModel class.It holds metadata about all the models defined that includes information about the tables, columns, and their relationships.
#create_all -This is a method of the metadata object that generates SQL commands to create all tables defined by the models. 
#engine - It is an argument uses to execute the commands. This function is crucial for setting up the initial state of the database
def create_db_tables_schema() -> None:
    SQLModel.metedata.create_all(engine)
#step 4 -getting_session function is responsible for providing or "getting" a session object.
#Session refers a class from SQLAlchemy that manage database transactions.It is a class that creates a session object.
#with statement ensures that resources are properly managed.Once the block of code under the with statement completes, the session is automatically closed, even if an error occurs.
def getting_session(): #No parameters
    with Session(engine) as session: #engine passed to the Session class to connect to the database.
        yield session #yield keyword is used in a generator function. It allows the function to return a value and pause its execution and resumed later.In this case, yield is used to produce the session object.
#Instead of returning the session object and exiting the function, yield provides the session object to the caller and allows the function to maintain its state. This is particularly useful in dependency injection scenarios where the caller of get_session might want to use the session for a while and then discard it when done.
#yield keyword turn the get_session function into a generator that allows the session to be provided to the caller in a lazy manner.
#Generator function - is defined with def but instead of using return to single value, it uses the yield keyword to yield multiple values one at a time
#Generators compute values on-the-fly as you iterate over them, rather than computing all values at once and storing them in memory