from sqlalchemy import create_engine, text


# connection parameters
host = "localhost"
port = "5432"
database = "postgres"
user = "postgres"
password = "mysecretpassword" 


def get_engine():
    """This function returns engine with its connection parameters"""
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")


