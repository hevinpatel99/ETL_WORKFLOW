"""
Module: patient_database_config

This module defines the `TABLE_CONFIG` dictionary, which specifies the schema
for various patient-related tables in a healthcare database. Each table is
represented as a dictionary, where the keys are column names and the values
represent the corresponding SQL data types.

Tables included:
- `patient_allergies`: Stores patient allergy information.
- `patient_info`: Stores general patient demographic details.
- `patient_diagnoses`: Records patient diagnoses.
- `patient_procedure`: Tracks medical procedures performed on patients.
- `patient_refills`: Logs prescription refill details.
- `patient_labs`: Stores lab test results for patients.
- `patient_meds`: Tracks prescribed medications and their details.
- `patient_family_history`: Records family medical history of patients.
- `patient_vitals`: Stores vital sign measurements.
- `patient_social_history`: Captures social history data like smoking status.

This configuration is intended to be used for database schema definition
or ORM-based table creation.
"""

TABLE_CONFIG = {
    "patient_allergies": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "STATEMENT": "TEXT",
        "TYPE": "TEXT",
        "ALLERGEN": "TEXT",
        "SYSTEM": "TEXT",
        "CODE": "TEXT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "REACTION": "TEXT",
        "SNOMED": "INT",
        "SEVERITY": "TEXT"
    },
    "patient_info": {
        "PID": "INT PRIMARY KEY",
        "GESTAGE": "FLOAT",
        "CITY": "TEXT",
        "APARTMENT": "TEXT",
        "STREET": "TEXT",
        "CELL": "TEXT",
        "DOB": "DATE",
        "GENDER": "TEXT",
        "REGION": "TEXT",
        "INITIAL": "TEXT",
        "YOB": "INT",
        "PCODE": "INT",
        "RACE": "TEXT",
        "HOME": "TEXT",
        "COUNTRY": "TEXT",
        "EMAIL": "TEXT",
        "LNAME": "TEXT",
        "FNAME": "TEXT"
    },
    "patient_diagnoses": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "SNOMED": "INT",
        "NAME": "TEXT"
    },
    "patient_procedure": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "SNOMED": "INT",
        "NAME": "TEXT",
        "NOTES": "TEXT"
    },
    "patient_refills": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "RXN": "INT",
        "DAYS": "INT",
        "QUANTITY": "INT"
    },
    "patient_labs": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "LOINC": "TEXT",
        "SCALE": "TEXT",
        "NAME": "TEXT",
        "VALUE": "DOUBLE",
        "LOW": "DOUBLE",
        "HIGH": "DOUBLE",
        "UNITS": "TEXT"
    },
    "patient_meds": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "RXNORM": "INT",
        "NAME": "TEXT",
        "SIG": "TEXT",
        "QUANTITY": "INT",
        "DAYS": "INT",
        "REFILLS": "INT",
        "Q_TO_TAKE_VALUE": "INT",
        "Q_TO_TAKE_UNIT": "TEXT",
        "FREQUENCY_VALUE": "INT",
        "FREQUENCY_UNIT": "TEXT"
    },
    "patient_family_history": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "RELATIVE_CODE": "INT",
        "RELATIVE_TITLE": "TEXT",
        "DATE_OF_BIRTH": "DATE",
        "DATE_OF_DEATH": "DATE",
        "PROBLEM_TITLE": "TEXT",
        "PROBLEM_CODE": "TEXT",
        "HEIGHT_CM": "DOUBLE"
    },
    "patient_vitals": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "ENCOUNTER_TYPE": "TEXT",
        "HEART_RATE": "INT",
        "RESPIRATORY_RATE": "INT",
        "TEMPERATURE": "DOUBLE",
        "WEIGHT": "DOUBLE",
        "HEIGHT": "DOUBLE",
        "BMI": "DOUBLE",
        "SYSTOLIC": "INT",
        "DIASTOLIC": "INT",
        "OXYGEN_SATURATION": "DOUBLE",
        "HEAD_CIRCUMFERENCE": "DOUBLE",
        "BP_SITE": "TEXT",
        "BP_METHOD": "TEXT",
        "BP_POSITION": "TEXT"
    },
    "patient_social_history": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "SMOKINGSTATUSCODE": "BIGINT"
    }
}
