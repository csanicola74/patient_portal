import dbm
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker  # https://faker.readthedocs.io/en/master/
import uuid
import random

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


# real cpt codes
cpt_codes = pd.read_csv(
    "https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv")
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(
    subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')


########## INSERTING IN FAKE PATIENTS ##########
# Approach 1: pandas to_sql
df_fake_patients.to_sql('patients', con=db_azure,
                        if_exists='append', index=False)
# query db_azure to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)

########## INSERTING IN FAKE CONDITIONS ##########

insertQuery = "INSERT INTO conditions (icd10_code, icd10_desc) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    db_azure.execute(
        insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    # stop once we have 50 rows
    if startingRow == 50:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM conditions", db_azure)

########## INSERTING IN FAKE PROCEDURES ##########

insertQuery = "INSERT INTO sx_procedure (proc_cpt, proc_desc) VALUES (%s, %s)"

procRowCount = 0
for index, row in cpt_codes_1k.iterrows():
    procRowCount += 1
    db_azure.execute(
        insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    # stop once we have 50 rows
    if procRowCount == 50:
        break

# cpt_codes_1k_moded = cpt_codes_1k.rename(columns={'com.medigy.persist.reference.type.clincial.CPT.code': 'proc_cpt', 'label': 'proc_desc'})
# cpt_codes_1k_moded = cpt_codes_1k_moded.drop(columns=['label'])
# ## keep only first 100 characters for each proc_desc
# cpt_codes_1k_moded['proc_desc'] = cpt_codes_1k_moded['proc_desc'].str[:100]

# cpt_codes_1k_moded.to_sql('procedure', con=db_azure, if_exists='replace', index=False)

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM sx_procedure", db_azure)


# now lets create some fake patient_conditions

# first, query conditions and patients to get the ids
df_conditions = pd.read_sql_query(
    "SELECT icd10_code FROM conditions", db_azure)
df_patients = pd.read_sql_query(
    "SELECT mrn FROM patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and place it in df_patient_conditions
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
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

# now lets create some fake patient_procedures

# first, lets query procedure and patients to get the ids

df_procedure = pd.read_sql_query(
    "SELECT proc_cpt FROM sx_procedure", db_azure)
df_patients = pd.read_sql_query(
    "SELECT mrn FROM patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of procedures between 1 and 5
df_patient_procedure = pd.DataFrame(columns=['mrn', 'proc_cpt'])
# for each patient in df_patient_procedure, take a random number of procedures between 1 and 10 from df_procedure and place it in df_patient_procedure
for index, row in df_patients.iterrows():
    # get a random number of procedure between 1 and 5
    numProcedures = random.randint(1, 5)
    # get a random sample of procedures from df_patient_procedure
    df_procedure_sample = df_procedure.sample(n=numProcedures)
    # add the mrn to the df_procedure_sample
    df_procedure_sample['mrn'] = row['mrn']
    # append the df_procedure_sample to df_patient_procedure
    df_patient_procedure = df_patient_procedure.append(
        df_procedure_sample)

print(df_patient_procedure)

# add a random procedure to each patient
insertQuery = "INSERT INTO patient_procedure (mrn, proc_cpt) VALUES (%s, %s)"

for index, row in df_patient_procedure.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['proc_cpt']))
    print("inserted row: ", index)

# now lets create some fake patient_medications
# first, lets query procedure and patients to get the ids

df_medications = pd.read_sql_query(
    "SELECT proc_cpt FROM medications", db_azure)
df_patients = pd.read_sql_query(
    "SELECT mrn FROM patients", db_azure)

# create a dataframe that is stacked and give each patient a random number of procedures between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'proc_cpt'])
# for each patient in df_patient_procedure, take a random number of procedures between 1 and 10 from df_procedure and place it in df_patient_procedure
for index, row in df_patients.iterrows():
    # get a random number of procedure between 1 and 5
    numProcedures = random.randint(1, 5)
    # get a random sample of procedures from df_patient_procedure
    df_procedure_sample = df_procedure.sample(n=numProcedures)
    # add the mrn to the df_procedure_sample
    df_procedure_sample['mrn'] = row['mrn']
    # append the df_procedure_sample to df_patient_procedure
    df_patient_procedure = df_patient_procedure.append(
        df_procedure_sample)

print(df_patient_procedure)

# add a random procedure to each patient
insertQuery = "INSERT INTO patient_procedure (mrn, proc_cpt) VALUES (%s, %s)"

for index, row in df_patient_procedure.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['proc_cpt']))
    print("inserted row: ", index)
