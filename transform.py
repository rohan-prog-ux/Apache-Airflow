import pandas as pd
import mysql.connector
from datetime import date, datetime, timedelta
def transform(responses):
    products=[]
    issues=[]
    sub_products=[]
    compliant_ids=[]
    timely=[]
    company_respopnse=[]
    submitted_via=[]
    comapnys=[]
    date_recive=[]
    state=[]
    sub_issue=[]
    for response in responses:
      try:
        for  hit in response['hits']['hits']:
            products.append(hit['_source']['product'])
            issues.append(hit['_source']['issue'])
            sub_products.append(hit['_source']['sub_product'])
            compliant_ids.append(int(hit['_source']['complaint_id']))
            timely.append(hit['_source']['timely'])
            company_respopnse.append(hit['_source']['company_response'])
            submitted_via.append(hit['_source']['submitted_via'])
            comapnys.append(hit['_source']['company'])
            date_recive.append(str(hit['_source']['date_received']).split('T')[0])
            state.append(hit['_source']['state'])
            sub_issue.append(hit['_source']['sub_issue'])
      except:
           print("key not found")
    list={'products':products,'issues':issues,'sub_products':sub_products,'compliant_ids':compliant_ids,'timely':timely,'company_response':company_respopnse,'submited_via':submitted_via,'company':comapnys,'date_recive':date_recive,'state':state,'sub_issue':sub_issue}
    df=pd.DataFrame(list)
    return df


