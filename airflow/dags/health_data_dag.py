from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import great_expectations as gx
import pandas as pd
import json
import os

# Read the data

def _get_data(**kwargs):
        
        # get each dataset on the basis of relative path
        df_patients =  gx.read_csv("./data/patients.csv")
        df_conditions =  gx.read_excel("./data/conditions.xlsx")
        df_encounters =  gx.read_parquet("./data/encounters.parquet")
        df_medications =  gx.read_csv("./data/medications.csv")
        df_symptoms =  gx.read_csv("./data/symptoms.csv")
        df_gender =  gx.read_csv("./data/patient_gender.csv")

        # Conveart the data frames to serailizable json so that data frames can be passed between DAG tasks using XCom
        df_patients = df_patients.to_json()
        df_conditions = df_conditions.to_json()
        df_encounters = df_encounters.to_json()
        df_medications = df_medications.to_json()
        df_symptoms = df_symptoms.to_json()
        df_gender = df_gender.to_json()

        # Push data to Xcom
        kwargs['ti'].xcom_push(key='df_patients', value=df_patients)
        kwargs['ti'].xcom_push(key='df_conditions', value=df_conditions)
        kwargs['ti'].xcom_push(key='df_encounters', value=df_encounters)
        kwargs['ti'].xcom_push(key='df_medications', value=df_medications)
        kwargs['ti'].xcom_push(key='df_symptoms', value=df_symptoms)
        kwargs['ti'].xcom_push(key='df_gender', value=df_gender)

        # print(df_gender.head(3))
        # print(df_patients.head(3))
              
# Process the data

def _clean_data(**kwargs):
        # Pull data from Xcom I will pull only those which I will process here
        df_patients = kwargs['ti'].xcom_pull(key='df_patients')
        df_gender = kwargs['ti'].xcom_pull(key='df_gender')
        df_symptoms = kwargs['ti'].xcom_pull(key='df_symptoms')  # The PK is missing I will add PK here

        # Convert data back to dataframe

        df_patients = pd.read_json(df_patients)
        df_gender = pd.read_json(df_gender)
        df_symptoms = pd.read_json(df_symptoms)

        # Update patients data with gender as the gender col in patients data is null
        merged_df = df_patients.merge(df_gender, left_on='PATIENT_ID', right_on='Id', how='left')
        df_patients['GENDER'] = merged_df['GENDER_y']

        # push updated patients data back to XCom
        df_patients = df_patients.to_json()
        kwargs['ti'].xcom_push(key='df_patients', value=df_patients)

        
        # Add PK to symptoms data set that is missing and push updated version to Xcom
        df_symptoms['ID'] = range(1, len(df_symptoms) + 1)
        df_symptoms.set_index('ID', inplace=True)
        df_symptoms.reset_index(inplace=True)
        df_symptoms = df_symptoms.to_json()
        kwargs['ti'].xcom_push(key='df_symptoms', value=df_symptoms)

# Structure the data according to Tuva  model

def _structure(**kwargs):
        # Pull dataframes from Xcom as  json
        
        df_patients = kwargs['ti'].xcom_pull(key='df_patients')
        df_conditions = kwargs['ti'].xcom_pull(key='df_conditions')
        df_encounters = kwargs['ti'].xcom_pull(key='df_encounters')
        df_medications = kwargs['ti'].xcom_pull(key='df_medications')
        df_symptoms = kwargs['ti'].xcom_pull(key='df_symptoms')
        
        # Convert json to data frame

        df_patients = pd.read_json(df_patients)
        df_conditions = pd.read_json(df_conditions)
        df_encounters = pd.read_json(df_encounters)
        df_medications = pd.read_json(df_medications)
        df_symptoms = pd.read_json(df_symptoms)

        # read the tuva schema mapping json file
        with open("./resources/tuva_schema_map.json", "r") as file:
                tuva_map = json.load(file)
        
        # print(tuva_map)
        
        # Create  directory to save the processed and mapped data with tuva input layer
        output_dir = "./processed_data"
        os.makedirs(output_dir, exist_ok=True)


        # Update the patients schema 
        # Drop the cols in patient data that are not in the tuva schema for patient dataset
        pat_drop_cols_list = ['SSN', 'DRIVERS', 'PASSPORT', 'PREFIX', 'SUFFIX', 'MAIDEN', 'MARITAL', 'BIRTHPLACE', 'HEALTHCARE_EXPENSES', 'HEALTHCARE_COVERAGE', 'INCOME']
        processed_patients = df_patients.drop(pat_drop_cols_list, axis=1)
        processed_patients.rename(columns=tuva_map["patient_map"], inplace=True)
        processed_patients.to_csv(os.path.join(output_dir, "patients.csv"), index=False)

        # Update conditions data
        df_conditions.rename(columns=tuva_map["condition_map"], inplace=True)
        df_conditions.to_csv(os.path.join(output_dir, "conditions.csv"), index=False)

        # Update encounter data
        enc_cols_to_remove = ["ORGANIZATION","PAYER"]
        processed_encounters = df_encounters.drop(enc_cols_to_remove, axis=1)
        processed_encounters.rename(columns=tuva_map["encounter_map"], inplace=True)
        processed_encounters.to_csv(os.path.join(output_dir, "encounters.csv"), index=False)

        # Update medication data
        med_cols_to_remove = ["PAYER", "BASE_COST", "PAYER_COVERAGE", "DISPENSES", "TOTALCOST", "REASONDESCRIPTION"]
        processed_medication = df_medications.drop(med_cols_to_remove, axis=1)
        processed_medication.rename(columns=tuva_map["medication_map"], inplace=True)
        processed_medication.to_csv(os.path.join(output_dir, "medications.csv"), index=False)

        # update symptoms data
        sym_cols_to_remove = ["GENDER", "RACE","ETHNICITY", "AGE_BEGIN", "AGE_END", "PATHOLOGY", "NUM_SYMPTOMS", "SYMPTOMS"]
        processed_symptom = df_symptoms.drop(sym_cols_to_remove, axis=1)
        processed_symptom.rename(columns=tuva_map["symptoms_map"], inplace=True)
        processed_symptom.to_csv(os.path.join(output_dir, "symptoms.csv"), index=False)

# Merge the processed data into master table 
        
def _merge():
        patients =  gx.read_csv("./processed_data/patients.csv")
        conditions =  gx.read_csv("./processed_data/conditions.csv")
        encounters =  gx.read_csv("./processed_data/encounters.csv")
        medications =  gx.read_csv("./processed_data/medications.csv")
        symptoms =  gx.read_csv("./processed_data/symptoms.csv")
        
        # Merging
        merged_df = pd.merge(patients, conditions, on='patient_id', how='left')
        merged_df = pd.merge(merged_df, encounters, on='patient_id', how='left')
        merged_df = pd.merge(merged_df, medications, on='patient_id', how='left')
        merged_df = pd.merge(merged_df, symptoms, on='patient_id', how='left')

        merged_df.to_csv(os.path.join("./processed_data", "merged_data.csv"), index=False)


with DAG("health_data", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        connect_data = PythonOperator(
            task_id = "connect_data",
            python_callable = _get_data,
            dag = dag,
        )

        clean_data = PythonOperator(
            task_id = "process_data",
            python_callable = _clean_data,
            dag = dag,
        )

        structure_data = PythonOperator(
            task_id="structure_data",
            python_callable=_structure,
            dag = dag,
        )

        merge_data = PythonOperator(
            task_id="merge_data",
            python_callable = _merge
        )

        status = BashOperator(
            task_id="status",
            bash_command="echo 'DONE'"
        )

        # define task order
        connect_data >> clean_data >> structure_data >> merge_data >> status