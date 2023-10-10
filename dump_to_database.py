import mysql.connector
import pandas as pd
import Extract as e
import load as l
import json
import transform as t
mydb=mysql.connector.connect(host='127.0.0.1',user='root',passwd='Azm123at@1',database='complains')





def dump_to_Database(responses):
    result=t.transform(responses)
    columns_name=result.columns
    sql =f'''CREATE TABLE IF NOT EXISTS Customer_complains(
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
    mycursor=mydb.cursor()
    try:
        #create the table if does not exit
        mycursor.execute(sql)  
        #create the table if does not exit 
        cols = ",".join([str(i) for i in result.columns.tolist()])
        #insert the rows
        for i,row in result.iterrows():
            sql = "INSERT INTO Customer_complains (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            mycursor.execute(sql,tuple(row))
            mydb.commit()    

    except:
        
        print("data not dump")
    return responses
