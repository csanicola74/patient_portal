#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dbm
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USER")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


########

connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

# show tables from databases
tableNames_azure = db_azure.table_names()

# reoder tables: patient_conditions, patient_procedure, procedure, patients, conditions
tableNames_azure = ['patient_conditions', 'patient_procedure',
                    'procedure', 'patients', 'conditions']

# first step below is just creating a basic version of each of the tables,
# along with the primary keys and default values

###
table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn int(8) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
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


table_patients_procedure = """
create table if not exists patients_procedure (
    id int auto_increment,
    mrn int(8) default null,
    proc_cpt varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (proc_cpt) REFERENCES sx_procedure(proc_cpt) ON DELETE CASCADE
); 
"""


table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn int(8) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""


db_azure.execute(table_patients)
db_azure.execute(table_sx_procedure)
db_azure.execute(table_conditions)
db_azure.execute(table_patients_procedure)
db_azure.execute(table_patient_conditions)


# get tables from db_azure
azure_tables = db_azure.table_names()


droppingFunction_limited(azure_tables, db_azure)


# confirm that it worked
# get tables from db_azure
azure_tables = db_azure.table_names()
