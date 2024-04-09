from typing import Union
import pandas
#import pandas-gbq
import pandas_gbq
from pandas.io import gbq
import os
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account


from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Welcome to the": "Globant Challenge"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/build/departments")
def build_results():
    # Read local files to upload
    local_ls =  [f for f in os.listdir("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//departments") if '.csv' in f]
    print(local_ls)
    # Define Datalake bucket
    bucket_name = "archivos-prueba-nalm"
    # Initiate storage Client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # First we will inherit permissions service account JSON
    blobs = bucket.list_blobs()
    filter_dir = ".json"

    file_name = [blob.name for blob in blobs if '.json' in blob.name ]

    number = sum(1 for _ in file_name)
    print('Número de JSON en bucket')
    print(number)
    # After we validate the existance of the service account file we download it
    counter = 0
    for f in file_name:
        # Download credentials file
        blob = bucket.get_blob(f)
        filepath=os.path.join(os.getcwd(),f.replace('credenciales JSON/',''))
        with open(filepath,'wb') as w:
            blob.download_to_file(w)
        w.close()
        print(f)
        counter+=1
        if counter == number:
            break
        else:
            print(f"Sigue el loop posición {counter}")
            
        


    credentials = service_account.Credentials.from_service_account_file(filepath)
    credentials = credentials.with_scopes(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    project = "mi-proyecto-prueba-375018"
    client = bigquery.Client(project = project, credentials = credentials)
    # We define the bucket where we will validate if the csv are already there
    bucket_name = "carga-csv-globant"


    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    #blobs = bucket.list_blobs(prefix='historic/departments')
    #blobs = bucket.list_blobs(prefix='historic')
    #print([blob.name for blob in blobs])
    bq_dataset = "historic"
    bq_table = "departments"

    #csv_name =  [blob.name for blob in blobs if 'players_bio_with_market_values' in blob.name if '2023-08-17' in blob.time_created]
    blobs = bucket.list_blobs(prefix='historic/departments')
    blob_names = [blob.name for blob in blobs]
    blob_names = blob_names[1:]
    blob_names = [blob.split('/')[-1] for blob in blob_names]
    print(blob_names)
    missing_names = [f for f in local_ls if f.replace('.csv','') not in blob_names]
    if len(missing_names) == 0:
        print("No new files")
        file_status = "No new files uploaded"
        return file_status
    else:
        csv = missing_names[0]
        print(csv)
        TABLE_ID = f'{project}.{bq_dataset}.{bq_table}'
        SCHEMA_LIST = []
        blob = bucket.blob("historic/departments/departments")
        blob.upload_from_filename("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//departments//" + csv )
        csv = csv.replace('.csv','')
        print(csv)
        hdw_schema = client.get_table(TABLE_ID)
        for field in hdw_schema.schema:
            SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
        job_config = bigquery.LoadJobConfig()
        job_config.schema = SCHEMA_LIST
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = ','
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.null_marker = ''
        ## Como la primera fila tiene el nombre de las columnas...
        job_config.skip_leading_rows = 1
        gcs_uri = f'gs://carga-csv-globant/historic/departments/{csv}'
        print(gcs_uri)
        load_job = client.load_table_from_uri(
            gcs_uri, TABLE_ID, job_config=job_config
        )

        load_job.result() # waits for the load job to finish

        destination_table = client.get_table(TABLE_ID)

        print(f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}')
        file_status = f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}'
        return file_status

    #df = pandas.read_csv("departments").T.to_dict()
    return {"Department": file_status}

@app.get("/build/jobs")
def build_results():
    # Read local files to upload
    local_ls =  [f for f in os.listdir("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//jobs") if '.csv' in f]
    print(local_ls)
    # Define Datalake bucket
    bucket_name = "archivos-prueba-nalm"
    # Initiate storage Client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # First we will inherit permissions service account JSON
    blobs = bucket.list_blobs()
    filter_dir = ".json"

    file_name = [blob.name for blob in blobs if '.json' in blob.name ]

    number = sum(1 for _ in file_name)
    print('Número de JSON en bucket')
    print(number)
    # After we validate the existance of the service account file we download it
    counter = 0
    for f in file_name:
        # Download credentials file
        blob = bucket.get_blob(f)
        filepath=os.path.join(os.getcwd(),f.replace('credenciales JSON/',''))
        with open(filepath,'wb') as w:
            blob.download_to_file(w)
        w.close()
        print(f)
        counter+=1
        if counter == number:
            break
        else:
            print(f"Sigue el loop posición {counter}")
            
        


    credentials = service_account.Credentials.from_service_account_file(filepath)
    credentials = credentials.with_scopes(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    project = "mi-proyecto-prueba-375018"
    client = bigquery.Client(project = project, credentials = credentials)
    # We define the bucket where we will validate if the csv are already there
    bucket_name = "carga-csv-globant"


    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    #blobs = bucket.list_blobs(prefix='historic/departments')
    #blobs = bucket.list_blobs(prefix='historic')
    #print([blob.name for blob in blobs])
    bq_dataset = "historic"
    bq_table = "jobs"

    blobs = bucket.list_blobs(prefix='historic/jobs')
    blob_names = [blob.name for blob in blobs]
    blob_names = blob_names[1:]
    blob_names = [blob.split('/')[-1] for blob in blob_names]
    print(blob_names)
    missing_names = [f for f in local_ls if f.replace('.csv','') not in blob_names]
    if len(missing_names) == 0:
        print("No new files")
        file_status = "No new files uploaded"
        return file_status
    else:
        csv = missing_names[0]
        print(csv)
        TABLE_ID = f'{project}.{bq_dataset}.{bq_table}'
        SCHEMA_LIST = []
        blob = bucket.blob("historic/jobs/" + csv.replace('.csv',''))
        blob.upload_from_filename("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//jobs//" + csv )
        csv = csv.replace('.csv','')
        print(csv)
        hdw_schema = client.get_table(TABLE_ID)
        for field in hdw_schema.schema:
            SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
        job_config = bigquery.LoadJobConfig()
        job_config.schema = SCHEMA_LIST
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = ','
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.null_marker = ''
        ## Como la primera fila tiene el nombre de las columnas...
        job_config.skip_leading_rows = 1
        gcs_uri = f'gs://carga-csv-globant/historic/jobs/{csv}'
        print(gcs_uri)
        load_job = client.load_table_from_uri(
            gcs_uri, TABLE_ID, job_config=job_config
        )

        load_job.result() # waits for the load job to finish

        destination_table = client.get_table(TABLE_ID)

        print(f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}')
        file_status = f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}'
        return file_status

    #df = pandas.read_csv("departments").T.to_dict()
    return {"jobs": file_status}

@app.get("/build/hired_employees")
def build_results():
    # Read local files to upload
    local_ls =  [f for f in os.listdir("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//hired_employees") if '.csv' in f]
    print(local_ls)
    # Define Datalake bucket
    bucket_name = "archivos-prueba-nalm"
    # Initiate storage Client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # First we will inherit permissions service account JSON
    blobs = bucket.list_blobs()
    filter_dir = ".json"

    file_name = [blob.name for blob in blobs if '.json' in blob.name ]

    number = sum(1 for _ in file_name)
    print('Número de JSON en bucket')
    print(number)
    # After we validate the existance of the service account file we download it
    counter = 0
    for f in file_name:
        # Download credentials file
        blob = bucket.get_blob(f)
        filepath=os.path.join(os.getcwd(),f.replace('credenciales JSON/',''))
        with open(filepath,'wb') as w:
            blob.download_to_file(w)
        w.close()
        print(f)
        counter+=1
        if counter == number:
            break
        else:
            print(f"Sigue el loop posición {counter}")
            
        


    credentials = service_account.Credentials.from_service_account_file(filepath)
    credentials = credentials.with_scopes(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    project = "mi-proyecto-prueba-375018"
    client = bigquery.Client(project = project, credentials = credentials)
    # We define the bucket where we will validate if the csv are already there
    bucket_name = "carga-csv-globant"


    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    #blobs = bucket.list_blobs(prefix='historic/departments')
    #blobs = bucket.list_blobs(prefix='historic')
    #print([blob.name for blob in blobs])
    bq_dataset = "historic"
    bq_table = "hired_employees"

    blobs = bucket.list_blobs(prefix='historic/hired_employees')
    blob_names = [blob.name for blob in blobs]
    blob_names = blob_names[1:]
    blob_names = [blob.split('/')[-1] for blob in blob_names]
    print(blob_names)
    missing_names = [f for f in local_ls if f.replace('.csv','') not in blob_names]
    if len(missing_names) == 0:
        print("No new files")
        file_status = "No new files uploaded"
        return file_status
    else:
        csv = missing_names[0]
        print(csv)
        TABLE_ID = f'{project}.{bq_dataset}.{bq_table}'
        SCHEMA_LIST = []
        blob = bucket.blob("historic/hired_employees/" + csv.replace('.csv',''))
        blob.upload_from_filename("C://Users//sr117//OneDrive//Documents//Globant//data_challenge_files//hired_employees//" + csv )
        csv = csv.replace('.csv','')
        print(csv)
        hdw_schema = client.get_table(TABLE_ID)
        for field in hdw_schema.schema:
            SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
        job_config = bigquery.LoadJobConfig()
        job_config.schema = SCHEMA_LIST
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = ','
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.null_marker = ''
        ## Como la primera fila tiene el nombre de las columnas...
        job_config.skip_leading_rows = 1
        gcs_uri = f'gs://carga-csv-globant/historic/hired_employees/{csv}'
        print(gcs_uri)
        load_job = client.load_table_from_uri(
            gcs_uri, TABLE_ID, job_config=job_config
        )

        load_job.result() # waits for the load job to finish

        destination_table = client.get_table(TABLE_ID)

        print(f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}')
        file_status = f'Cargadas {destination_table.num_rows} filas a tabla {bq_table}'
        return file_status

    #df = pandas.read_csv("departments").T.to_dict()
    return {"hired_employees": file_status}

@app.get("/employee_number/{year}")
def employee_number(year: int, q: Union[str, None] = None):
    #http://127.0.0.1:8000/employee_number/2021
    # Define Datalake bucket
    bucket_name = "archivos-prueba-nalm"
    # Initiate storage Client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # First we will inherit permissions service account JSON
    blobs = bucket.list_blobs()
    filter_dir = ".json"

    file_name = [blob.name for blob in blobs if '.json' in blob.name ]

    number = sum(1 for _ in file_name)
    print('Número de JSON en bucket')
    print(number)
    # After we validate the existance of the service account file we download it
    counter = 0
    for f in file_name:
        # Download credentials file
        blob = bucket.get_blob(f)
        filepath=os.path.join(os.getcwd(),f.replace('credenciales JSON/',''))
        with open(filepath,'wb') as w:
            blob.download_to_file(w)
        w.close()
        print(f)
        counter+=1
        if counter == number:
            break
        else:
            print(f"Sigue el loop posición {counter}")
            
        


    credentials = service_account.Credentials.from_service_account_file(filepath)
    credentials = credentials.with_scopes(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    project = "mi-proyecto-prueba-375018"
    client = bigquery.Client(project = project, credentials = credentials)
    employee_number_query = f"""
    SELECT department, job, 
    COUNT(CASE WHEN EXTRACT(QUARTER FROM DATE (hire_date)) = 1 THEN employee_code ELSE NULL END) as Q1,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM DATE (hire_date)) = 2 THEN employee_code ELSE NULL END) as Q2,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM DATE (hire_date)) = 3 THEN employee_code ELSE NULL END) as Q3,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM DATE (hire_date)) = 4 THEN employee_code ELSE NULL END) as Q4
    FROM `mi-proyecto-prueba-375018.historic.departments` t1
    JOIN `mi-proyecto-prueba-375018.historic.hired_employees` t2 ON t1.code = t2.department_id
    JOIN `mi-proyecto-prueba-375018.historic.jobs` t3 ON t2.job_id = t3.job_code
    WHERE EXTRACT(YEAR FROM DATE (hire_date)) = {year}
    GROUP BY department, job
    ORDER BY department ASC, job ASC
    
    """
    #query_job = client.query(employee_number_query)
    #results = query_job.result()
    df = gbq.read_gbq(employee_number_query, project_id=project, dialect="standard")
    #print(df)
    return {"df":df.T.to_dict(),"year": year}

@app.get("/mean_employee_number/{year}")
def mean_employee_number(year: int, q: Union[str, None] = None):
    #http://127.0.0.1:8000/mean_employee_number/2021
    # Define Datalake bucket
    bucket_name = "archivos-prueba-nalm"
    # Initiate storage Client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # First we will inherit permissions service account JSON
    blobs = bucket.list_blobs()
    filter_dir = ".json"

    file_name = [blob.name for blob in blobs if '.json' in blob.name ]

    number = sum(1 for _ in file_name)
    print('Número de JSON en bucket')
    print(number)
    # After we validate the existance of the service account file we download it
    counter = 0
    for f in file_name:
        # Download credentials file
        blob = bucket.get_blob(f)
        filepath=os.path.join(os.getcwd(),f.replace('credenciales JSON/',''))
        with open(filepath,'wb') as w:
            blob.download_to_file(w)
        w.close()
        print(f)
        counter+=1
        if counter == number:
            break
        else:
            print(f"Sigue el loop posición {counter}")
            
        


    credentials = service_account.Credentials.from_service_account_file(filepath)
    credentials = credentials.with_scopes(
        [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    project = "mi-proyecto-prueba-375018"
    client = bigquery.Client(project = project, credentials = credentials)
    mean_hired_employees = []
    mean_query = f"""
    SELECT COUNT(DISTINCT employee_code)/COUNT(DISTINCT code) as mean_hired_employees
    FROM `mi-proyecto-prueba-375018.historic.departments` t1
    JOIN `mi-proyecto-prueba-375018.historic.hired_employees` t2 ON t1.code = t2.department_id
    JOIN `mi-proyecto-prueba-375018.historic.jobs` t3 ON t2.job_id = t3.job_code
    WHERE EXTRACT(YEAR FROM DATE (hire_date)) = {year}
    ORDER BY mean_hired_employees DESC
    """
    query_job = client.query(mean_query)
    results = query_job.result()
    for row in results:
        mean_hired_employees.append(row.mean_hired_employees)
    mean_hired_employees = mean_hired_employees[0]
    employee_number_query = f"""
    SELECT code, department, COUNT(employee_code) as hired_employees
    FROM `mi-proyecto-prueba-375018.historic.departments` t1
    JOIN `mi-proyecto-prueba-375018.historic.hired_employees` t2 ON t1.code = t2.department_id
    JOIN `mi-proyecto-prueba-375018.historic.jobs` t3 ON t2.job_id = t3.job_code
    WHERE EXTRACT(YEAR FROM DATE (hire_date)) = {year}
    GROUP BY code, department
    HAVING COUNT(employee_code) >= {mean_hired_employees}
    ORDER BY hired_employees DESC
    
    """
    #query_job = client.query(employee_number_query)
    #results = query_job.result()
    df = gbq.read_gbq(employee_number_query, project_id=project, dialect="standard")
    return {"df":df.T.to_dict(),"year": year}
