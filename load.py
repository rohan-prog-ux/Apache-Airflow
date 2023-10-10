import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import mysql.connector
def load():
    mydb=mysql.connector.connect(host='127.0.0.1',user='root',passwd='12345',database='mydb')
    mycursor=mydb.cursor()
    mycursor.execute('SELECT * FROM mydb.customer_complains ')
    result=mycursor.fetchall()
    colums_name=['products','issues','sub_products','compliant_ids','timely','company_respopnse','submitted_via','comapnys','date_recive','state','sub_issue']
    df=pd.DataFrame(result,columns=colums_name)
    load_1()
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'] #os.getenv('SCOPE')
    SERVICE_ACCOUNT_FILE = '/Users/mac/Desktop/dag .json'#os.getenv('SERVICE_ACCOUNT_FILE')
    #SAMPLE_SPREADSHEET_ID = '1JDttND5GyfWqUQ_vG-EmLelfh7JxGLb3VR6LkjyV9LY'#os.getenv('SAMPLE_SPREADSHEET_ID')
    cred=None
    cred = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
    service = build('sheets', 'v4', credentials=cred)
    sheet =  service.spreadsheets()


    sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,range='state!A1',
                        valueInputOption="RAW",body={'majorDimension':'ROWS',
                        'values':df.T.reset_index().T.values.tolist()}).execute()



