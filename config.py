import configparser 
import psycopg2  as pdb
import pyodbc
from sqlalchemy import create_engine

def config_postgre_for_pandas_frame(file_name: str = "databases.ini", section: str = "postgresql"):
    parser = configparser.ConfigParser()
    parser.read(file_name)
    db_session = {}
    
    if parser.has_section(section):
        parameters = parser.items(section)
        for param in parameters:
            db_session[param[0]] = param[1]
    else:
        raise Exception(f"Section '{section}' is not in the '{file_name}' file")

    # Create SQLAlchemy engine
    connection_string = f"postgresql://{db_session['user']}:{db_session['password']}@{db_session['host']}:{db_session['port']}/{db_session['database']}"
    return create_engine(connection_string)

def config_postgre(file_name = "databases.ini", section="postgresql"):
    #creating a parser
    parser = configparser.ConfigParser()
    #read config file
    parser.read(file_name)
    db_session ={}
    #check if database section iscreated or not
    if parser.has_section(section):
        parameters = parser.items(section)
        for param in parameters :
            db_session[param[0]] = param[1]
    else:
        raise Exception(
            "section{0} is not in the {1} file".format(section,file_name)
        )
    return pdb.connect(**db_session) 

def config_sql_server(file_name = "databases.ini", section="sqlserver"):
    #creating a parser
    parser = configparser.ConfigParser()
    #read config file
    parser.read(file_name)
    db_session ={}
    #check if database section iscreated or not
    if parser.has_section(section):
        parameters = parser.items(section)
        for param in parameters :
            db_session[param[0]] = param[1]
    else:
        raise Exception(
            "section{0} is not in the {1} file".format(section,file_name)
        )
    return pyodbc.connect(**db_session)

