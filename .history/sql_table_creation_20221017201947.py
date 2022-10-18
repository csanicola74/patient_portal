import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


patient_portal = pd.DataFrame(
    ['patients', 'medications', 'treatments_procedures', 'conditions', 'social_determinants'])


load_dotenv()
AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USER")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


connection_string = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}/{AZURE_MYSQL_DATABASE}'
engine = create_engine(connection_string)

# Test connection
print(engine.table_names())

# create tables
patient_portal = pd.DataFrame(
    ['patients', 'medications', 'treatments_procedures', 'conditions', 'social_determinants'])


###
portal_conditions = """
create table conditions (
    id int, 
    mrn, """
