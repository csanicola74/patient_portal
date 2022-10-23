#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dbm
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker  # https://faker.readthedocs.io/en/master/
import uuid
import random


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


connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string_gcp)

connection_string_gcp_2 = f'mysql+pymysql://{GCP_MYSQL_USER_2}:{GCP_MYSQL_PASSWORD_2}@{GCP_MYSQL_HOSTNAME_2}:3306/{GCP_MYSQL_DATABASE_2}'
db_gcp_2 = create_engine(connection_string_gcp_2)

# show databases
print(db_azure.table_names())
print(db_gcp.table_names())
print(db_gcp_2.table_names())


# fake stuff
fake = Faker()

fake_patients = [
    {
        # keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8],
        'first_name':fake.first_name(),
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(10)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


# real icd10 codes
icd10codes = pd.read_csv(
    'https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000, random_state=1)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(
    subset=['CodeWithSeparator'], keep='first')


# real ndc codes
ndc_codes = pd.read_csv(
    'https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(
    subset=['PRODUCTNDC'], keep='first')


########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########


# Approach 1: pandas to_sql
# Approach 1: pandas to_sql
# Approach 1: pandas to_sql
# Approach 1: pandas to_sql


# df_fake_patients.to_sql('production_patients', con=db_azure, if_exists='append', index=False)
# df_fake_patients.to_sql('production_patients', con=db_gcp, if_exists='append', index=False)

# # query db_azure to see if data is there
# df_azure = pd.read_sql_query("SELECT * FROM production_patients", db_azure)
# db_gcp = pd.read_sql_query("SELECT * FROM production_patients", db_gcp)

# Approach 2: sqlalchemy with dynamic modification of values
# Approach 2: sqlalchemy with dynamic modification of values
# Approach 2: sqlalchemy with dynamic modification of values
# Approach 2: sqlalchemy with dynamic modification of values

insertQuery = "INSERT INTO production_patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'],
                     row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    db_gcp.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'],
                   row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)

# # query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_patients", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_patients", db_gcp)


########## INSERTING IN FAKE CONDITIONS ##########

insertQuery = "INSERT INTO production_conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    db_azure.execute(
        insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    db_gcp.execute(
        insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    # stop once we have 50 rows
    if startingRow == 50:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_conditions", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_conditions", db_gcp)

# ###### the above way is inefficient, but it works.
# ###### below is better if we have thousands/millions of rows to insert
# ##### for, for these big pushes, recommend using pandas to_sql, to do this, just need to make sure column names first match
# icd10codesShort_1k_mod = icd10codesShort_1k.rename(columns={'CodeWithSeparator': 'icd10_code', 'ShortDescription': 'icd10_description'})
# icd10codesShort_1k_mod.to_sql('production_conditions', con=db_azure, if_exists='replace', index=False)
# icd10codesShort_1k_mod.to_sql('production_conditions', con=db_gcp, if_exists='replace', index=False)


########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########

insertQuery = "INSERT INTO production_medications (med_ndc, med_human_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(
        insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    db_gcp.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    # stop once we have 50 rows
    if medRowCount == 50:
        break

# ndc_codes_1k_moded = ndc_codes_1k.rename(columns={'PRODUCTNDC': 'med_ndc', 'NONPROPRIETARYNAME': 'med_human_name'})
# ndc_codes_1k_moded = ndc_codes_1k_moded.drop(columns=['PROPRIETARYNAME'])
# ## keep only first 100 characters for each med_human_name
# ndc_codes_1k_moded['med_human_name'] = ndc_codes_1k_moded['med_human_name'].str[:100]

# ndc_codes_1k_moded.to_sql('production_medications', con=db_azure, if_exists='replace', index=False)
# ndc_codes_1k_moded.to_sql('production_medications', con=db_gcp, if_exists='replace', index=False)

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_medications", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_medications", db_gcp)


# now lets create some fake patient_conditions

# first, lets query production_conditions and production_patients to get the ids
df_conditions = pd.read_sql_query(
    "SELECT icd10_code FROM production_conditions", db_azure)
df_patients = pd.read_sql_query(
    "SELECT mrn FROM production_patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=numConditions)
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions)

# now lets add a random condition to each patient
insertQuery = "INSERT INTO production_patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)


# now lets create some fake patient_medications

# first, lets query production_medications and production_patients to get the ids

df_medications = pd.read_sql_query(
    "SELECT med_ndc FROM production_medications", db_azure)
df_patients = pd.read_sql_query(
    "SELECT mrn FROM production_patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(
        df_medications_sample)

print(df_patient_medications)

# now lets add a random medication to each patient
insertQuery = "INSERT INTO production_patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)


# try and insert a new row with a random mrn and a random icd10_code
db_azure.execute(insertQuery, (random.randint(1, 1000000),
                 random.choice(df_conditions['icd10_code'])))
# what happens and why?


####  INSERTING IN REAL DATA  ####
icd10codes = pd.read_csv()

cpt_codes = pd.read_csv(
    "https://gist.github.com/lieldulev/439793dc3c5a6613b661c33d71fdd185")
