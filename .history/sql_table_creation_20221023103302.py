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
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")

GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")

GCP_MYSQL_HOSTNAME_2 = os.getenv("GCP_MYSQL_HOSTNAME_2")
GCP_MYSQL_USER_2 = os.getenv("GCP_MYSQL_USERNAME_2")
GCP_MYSQL_PASSWORD_2 = os.getenv("GCP_MYSQL_PASSWORD_2")
GCP_MYSQL_DATABASE_2 = os.getenv("GCP_MYSQL_DATABASE_2")


########

connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

# show tables from databases
tableNames_azure = db_azure.table_names()

# reoder tables: production_patient_conditions, production_patient_medications, production_medications, production_patients, production_conditions
tableNames_azure = ['production_patient_conditions', 'production_patient_medications',
                    'production_medications', 'production_patients', 'production_conditions']

# ### delete everything
droppingFunction_all(tableNames_azure, db_azure)
droppingFunction_all(tableNames_gcp, db_gcp)


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


table_procedure = """
create table if not exists procedure (
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


table_prod_patients_medications = """
create table if not exists production_patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES production_medications(med_ndc) ON DELETE CASCADE
); 
"""


table_prod_patient_conditions = """
create table if not exists production_patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES production_patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES production_conditions(icd10_code) ON DELETE CASCADE
); 
"""


db_gcp.execute(table_prod_patients)
db_gcp.execute(table_prod_medications)
db_gcp.execute(table_prod_conditions)
db_gcp.execute(table_prod_patients_medications)
db_gcp.execute(table_prod_patient_conditions)

db_gcp_2.execute(table_prod_patients)
db_gcp_2.execute(table_prod_medications)
db_gcp_2.execute(table_prod_conditions)
db_gcp_2.execute(table_prod_patients_medications)
db_gcp_2.execute(table_prod_patient_conditions)


db_azure.execute(table_prod_patients)
db_azure.execute(table_prod_medications)
db_azure.execute(table_prod_conditions)
db_azure.execute(table_prod_patients_medications)
db_azure.execute(table_prod_patient_conditions)


# get tables from db_azure
azure_tables = db_azure.table_names()

# get tables from db_gcp
gcp_tables = db_gcp.table_names()


droppingFunction_limited(azure_tables, db_azure)
droppingFunction_limited(gcp_tables, db_gcp)


# confirm that it worked
# get tables from db_azure
azure_tables = db_azure.table_names()
# get tables from db_gcp
gcp_tables = db_gcp.table_names()
