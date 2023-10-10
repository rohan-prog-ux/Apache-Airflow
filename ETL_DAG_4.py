from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
from datetime import date, datetime, timedelta
import json
import pandas as pd
from airflow import DAG
# from airflow.decorators import dag, task
import os

import mysql.connector

default_args = {
    'owner': 'rohan',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 10),
    'retries': 1
}

with DAG(dag_id='ETL_DAG_4',
         default_args=default_args,
         start_date=datetime(2023, 2, 10),
         schedule_interval='@daily') as dag:



def ETL_DAG_4():

    @task()

    def Extract():

        url = base_url = "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf"

        payload = {}
        headers = {'Content-Type': 'application/json'}

        list_of_state = json.loads(requests.request(
            "GET", url, headers=headers, data=payload).text)
        size = 500
        time_delta = 365
        max_date = (date.today()).strftime("%Y-%m-%d")
        min_date = (date.today() - timedelta(days=time_delta)
                    ).strftime("%Y-%m-%d")
        response = []
        for key, value in list_of_state.items():
            url = f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={size}&date_received_max={max_date}&date_received_min={min_date}&state={key}'


            response.append(requests.get(url).json())

        return response

    @task()

    def dump_to_Database(responses):
        mydb = mysql.connector.connect(
            host='127.0.0.1', user='root', passwd='Waheguru123', database='mydb')
        result = Transform(responses)
        columns_name = result.columns
        sql = f'''CREATE TABLE IF NOT EXISTS Customer_complains(
        {columns_name[0]} CHAR(255) NOT NULL,
        {columns_name[1]} CHAR(225),
        {columns_name[2]} CHAR(225),
        {columns_name[3]}  INT NOT NULL ,
        {columns_name[4]}  CHAR(225),
        {columns_name[5]}  CHAR(255),
        {columns_name[6]}  CHAR(255),
        {columns_name[7]}  CHAR(255),
        {columns_name[8]}  CHAR(255),
        {columns_name[9]}  CHAR(255),
        {columns_name[10]}  CHAR(255)
        )'''
        mycursor = mydb.cursor()
        try:
            
            mycursor.execute(sql)
            cols = ",".join([str(i) for i in result.columns.tolist()])
            for i, row in result.iterrows():
                sql = "INSERT INTO Customer_complains (" + cols + \
                    ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                mycursor.execute(sql, tuple(row))
                mydb.commit()

        except:

            print("data not dump")
        mydb = mysql.connector.connect(
            host='127.0.0.1', user='root', passwd='Waheguru123', database='mydb')
        mycursor = mydb.cursor()
        mycursor.execute('SELECT * FROM mydb.customer_complains ')
        result = mycursor.fetchall()
        colums_name = ['products', 'issues', 'sub_products', 'compliant_ids', 'timely',
                       'company_respopnse', 'submitted_via', 'comapnys', 'date_recive', 'state', 'sub_issue']
        df = pd.DataFrame(result, columns=colums_name)
        return responses

    @task()
    def Transform(responses):
        products = []
        issues = []
        sub_products = []
        compliant_ids = []
        timely = []
        company_respopnse = []
        submitted_via = []
        comapnys = []
        date_recive = []
        state = []
        sub_issue = []
        for response in responses:
            try:
                for hit in response['hits']['hits']:
                    products.append(hit['_source']['product'])
                    issues.append(hit['_source']['issue'])
                    sub_products.append(hit['_source']['sub_product'])
                    compliant_ids.append(hit['_source']['complaint_id'])
                    timely.append(hit['_source']['timely'])
                    company_respopnse.append(
                        hit['_source']['company_response'])
                    submitted_via.append(hit['_source']['submitted_via'])
                    comapnys.append(hit['_source']['company'])
                    date_recive.append(
                        str(hit['_source']['date_received']).split('T')[0])
                    state.append(hit['_source']['state'])
                    sub_issue.append(hit['_source']['sub_issue'])
            except:
                print("key not found")
                list = {'products': products, 'issues': issues, 'sub_products': sub_products, 'compliant_ids': compliant_ids, 'timely': timely,
                  'company_response': company_respopnse, 'submited_via': submitted_via, 'company': company, 'date_recive': date_recive, 'state': state, 'sub_issue': sub_issue}
        df = pd.DataFrame(list)
        return df

    @task()
    def Load(df):

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive'] 
        
        SERVICE_ACCOUNT_FILE = 'user/mac/CDE/Dag/assignment.json'
        # SAMPLE_SPREADSHEET_ID = '1JDttND5GyfWqUQ_vG-EmLelfh7JxGLb3VR6LkjyV9LY'
        cred = None
        cred = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        service = build('sheets', 'v4', credentials=cred)
        sheet = service.spreadsheets()

        sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='state!A1',
                              valueInputOption="RAW", body={'majorDimension': 'ROWS',
                                                            'values': df.T.reset_index().T.values.tolist()}).execute()

    responses = Extract()
    dump_data = dump_data_to_Database(responses)
    df = Transform(dump_data)
    Load(df)


etl_dag = ETL_DAG_4()
