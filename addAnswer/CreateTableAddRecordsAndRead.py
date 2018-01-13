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
        exec_id = event['exec_id']
        answer = [];
        answerData = event['answer'];
        
        if answerData != "" :
            answer.append(exec_id)
            answer.append(answerData['name'])
            for i in range(0, len(answerData['data'])):
                answer.append(answerData['data'][i])
            for i in range(len(answerData['data'])+2, 22):
                answer.append("NULL")
            print(answer)
            sqlAddAnswer = 'insert into answers (exec_id, name, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)';
            cursor.execute(sqlAddAnswer,answer);
            conn.commit()
      
        sqlGetDayListId = "select title from indexs where exec_id=%(exec_id)s";
        cursor.execute(sqlGetDayListId, { 'exec_id' : exec_id});
        rows = cursor.fetchone()
        for row in rows:
            dayListId = row
        print(dayListId)
        
        sqlGetDayList = "select * from dayLists where exec_id=%(exec_id)s";
        cursor.execute(sqlGetDayList, { 'exec_id': exec_id});
        rows = cursor.fetchall()
        for row in rows:
            dayList = row
        dayListResponse = []
        for i in range(1, len(dayList)):
            logger.info(dayList[i])
            if dayList[i] == 'NULL':
                break;
            dayListResponse.append(dayList[i])
        print(dayListResponse)
            
        locListResponse = []
        
        sqlGetAnswers = 'select * from answers where exec_id=%(exec_id)s';
        cursor.execute(sqlGetAnswers, { 'exec_id' : exec_id});
        print('here1')
        rows = cursor.fetchall()
        answerData = []
        print('here2')
        for row in rows:
            answers = []
            for i in range(2, len(row)):
                logger.info(row[i])
                if row[i] == 'NULL':
                    break;
                answers.append(row[i])
            data = { "name" : row[1], "data" : answers}
            answerData.append(data)
            
        response = { "daysList" : dayListResponse, "locationList" : locListResponse, "answersData" : answerData}
        json_results = json.dumps(response, ensure_ascii=False)
        
        cursor.close()
        conn.close()
    except:
        logger.error("ERROR: DB ERROR.")
        return []
    return json_results