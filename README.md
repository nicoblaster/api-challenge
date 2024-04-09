# api-challenge
A repository for the API challenge

## Notes and Considerations

Usually I would have designed an API key to access the endpoints, as they use a service account with control to a personal proyect.
If this were a product for a client, the API Key would be given only to those with permitted access, and the service account used would be that of a relevant proyect,
with data focused on client requirements and permissions.

## First Install the required libraries

$ pip install fastapi

$ pip install uvicorn

$ pip install pandas-gbq

## Run the application

$ uvicorn main:app --reload

## Then, decide the required endpoint

### Data csv cases

In the case of the upload of csv files to the SQL solution (Google BigQuery), the local folder structure required is the folowing:
LOCAL/PATHTOFOLDER/hired_employees/hired_employees.csv

The endpoint paths are the following:
For departments:
http://127.0.0.1:8000/build/departments
For hired_employees:
http://127.0.0.1:8000/build/hired_employees
For jobs:
http://127.0.0.1:8000/build/jobs

### SQL Query cases

The number can be changed depending on the year of interest
The endpoint paths are the following:

For number of employees hired department by quarter:
http://127.0.0.1:8000/employee_number/2021
For departments with hirings above the mean:
http://127.0.0.1:8000/mean_employee_number/2021
