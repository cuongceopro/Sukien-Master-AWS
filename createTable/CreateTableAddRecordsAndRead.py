import sys
import logging
import mysql.connector
import boto3
import os
import json
import rds_config
import re

RDS_HOST = rds_config.rds_host
RDS_USER = rds_config.rds_user
RDS_REGION = rds_config.rds_region
RDS_DB_NAME = 'sukien_master'
RDS_CHARSET = 'utf8'
RDS_PASSWORD = rds_config.rds_password

rds = boto3.client('rds')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = mysql.connector.connect(
        user=RDS_USER,
        password=RDS_PASSWORD,
        host=RDS_HOST,
        database=RDS_DB_NAME,
        charset=RDS_CHARSET
    )
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

def handler(event, context):
    
    cursor = conn.cursor(dictionary=False, buffered=False)
    logger.info("SUCCESS: event is OK")

    try:

        sqlGetMaxExecId = "SELECT MAX(id) AS id FROM indexs";
        cursor.execute(sqlGetMaxExecId);
        rows = cursor.fetchone()
        for row in rows:
            maxExecId = row
        print(maxExecId)
        
        newExecId = '011000010110001001100011' + str(maxExecId+1) + '000110010101100110011001110000'; 
        print(newExecId)
        
        title = event['title']
        memo = event['memo']
        
        sqlAddEvent = 'insert into indexs (exec_id, title, memo) values(%(exec_id)s, %(title)s, %(memo)s)';
        cursor.execute(sqlAddEvent, {'exec_id' : newExecId, 'title' : title, 'memo' : memo });
        
        dayList = []
        dayListData = event['time']
        dayList.append(newExecId)
        for i in range(0, len(dayListData)):
            dayList.append(dayListData[i])
        for i in range(len(dayListData)+2, 22):
            dayList.append("NULL")
        print(dayList)
        sqlAddDayList = 'insert into dayLists (exec_id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)';
        cursor.execute(sqlAddDayList,dayList);
        
        conn.commit()
        cursor.close()
        conn.close()
    except:
        logger.error("ERROR: DB ERROR. ahihi")
    return newExecId