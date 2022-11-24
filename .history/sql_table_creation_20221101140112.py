import dbm
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

####  CONNECT TO AZURE MYSQL SERVER  ####
AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USER")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


########

connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

####  SHOW ALL THE TABLES FROM THE DATABASE  ####
tableNames_azure = db_azure.table_names()

# reorder tables: patient_conditions, patient_procedure, sx_procedure, patients, conditions
tableNames_azure = ['patient', 'conditions', 'medications', 'sx_procedure',
                    'patient_conditions', 'patient_medications', 'patient_procedure']


# first step below is just creating a basic version of each of the tables,
# along with the primary keys and default values

####  CREATE TABLES  ####
table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    insurance varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_sx_procedure = """
create table if not exists sx_procedure (
    id int auto_increment,
    proc_cpt varchar(255) default null unique,
    proc_desc varchar(255) default null,
    PRIMARY KEY (id)
);
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_desc varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_patient_procedure = """
create table if not exists patient_procedure (
    id int auto_increment,
    mrn varchar(255) default null,
    proc_cpt varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (proc_cpt) REFERENCES sx_procedure(proc_cpt) ON DELETE CASCADE
); 
"""

table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""

table_patients_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

db_azure.execute(table_patients)
db_azure.execute(table_sx_procedure)
db_azure.execute(table_conditions)
db_azure.execute(table_medications)
db_azure.execute(table_patient_procedure)
db_azure.execute(table_patient_conditions)
db_azure.execute(table_patients_medications)


# get tables from db_azure
azure_tables = db_azure.table_names()
