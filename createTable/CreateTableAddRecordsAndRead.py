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
rds = boto3.client('rds')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    password='myfavourite'

    conn = mysql.connector.connect(
        user=RDS_USER,
        password=password,
        host=RDS_HOST,
        database=RDS_DB_NAME,
        charset=RDS_CHARSET
        # ssl_verify_cert=True,
        # ssl_ca='{}/rds-combined-ca-bundle.pem'.format(os.environ['LAMBDA_TASK_ROOT'])
    )
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

def handler(event, context):
    
    cursor = conn.cursor(dictionary=False, buffered=False)
    logger.info(cursor)
    logger.info("SUCCESS: event is OK")

    try:
        # sql = "show tables";
        
        # sql0 = 'insert into Employee3 (EmpID, Name) values(7, "Joe")';
        # sql1 = 'insert into Employee3 (EmpID, Name) values(8, "Bob")';
        # sql2 = 'insert into Employee3 (EmpID, Name) values(9, "Mary")';
        # sql3 = "select * from Employee3";
        
        # sql = "create table aaa ( exec_id  varchar(255) NOT NULL, dayListId MEDIUMINT NOT NULL , locListId MEDIUMINT NOT NULL)"
        # sql0 = 'insert into app (exec_id) values("abcabcd")';
        # sql1 = 'insert into app (exec_id) values("bcdbcdd")';
        # sql2 = 'insert into app (exec_id) values("cdecded")';
        # sql3 = "select * from apps where exec_id=%(exec_id)s";       
        
        # sql = "create table dayLists (id int NOT NULL, op1 varchar(255), op2 varchar(255), op3 varchar(255), op4 varchar(255), op5 varchar(255), op6 varchar(255), op7 varchar(255), op8 varchar(255), op9 varchar(255), op10 varchar(255), op11 varchar(255), op12 varchar(255), op13 varchar(255), op14 varchar(255), op15 varchar(255), op16 varchar(255), op17 varchar(255), op18 varchar(255), op19 varchar(255), op20 varchar(255), PRIMARY KEY (id))"
        # sql0 = 'insert into dayLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(0, "10/01", "10/02", "10/03", "10/04", "10/05", "10/06", "10/07", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql1 = 'insert into dayLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(1, "10/07", "10/08", "10/09", "10/10", "10/05", "10/06", "10/07", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql2 = 'insert into dayLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(2, "10/08", "10/08", "10/09", "10/10", "10/05", "10/06", "10/17", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql3 = "select * from dayLists";
        
        # sql = "create table locLists (id int NOT NULL, op1 varchar(255), op2 varchar(255), op3 varchar(255), op4 varchar(255), op5 varchar(255), op6 varchar(255), op7 varchar(255), op8 varchar(255), op9 varchar(255), op10 varchar(255), op11 varchar(255), op12 varchar(255), op13 varchar(255), op14 varchar(255), op15 varchar(255), op16 varchar(255), op17 varchar(255), op18 varchar(255), op19 varchar(255), op20 varchar(255), PRIMARY KEY (id))"
        # sql0 = 'insert into locLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(0, "Tokyo", "Osaka", "Fukuoka", "Hokkaido", "Shizuoka", "Hiroshima", "Okinawa", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql1 = 'insert into locLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(1, "Osaka", "Tokyo", "Hokkaido", "Shizuoka", "Okinawa", "Hiroshima", "Tokyo", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql2 = 'insert into locLists (id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(2, "Fukuoka", "Osaka", "Okinawa", "Tokyo", "Hiroshima", "Hokkaido", "Shizuoka", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql3 = "select * from locLists";
        
        # sql = "create table answers (exec_id varchar(255) NOT NULL, name varchar(255) NOT NULL, op1 varchar(255), op2 varchar(255), op3 varchar(255), op4 varchar(255), op5 varchar(255), op6 varchar(255), op7 varchar(255), op8 varchar(255), op9 varchar(255), op10 varchar(255), op11 varchar(255), op12 varchar(255), op13 varchar(255), op14 varchar(255), op15 varchar(255), op16 varchar(255), op17 varchar(255), op18 varchar(255), op19 varchar(255), op20 varchar(255))"
        # sql0 = 'insert into answers (exec_id, name, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values("abcabcd", "Yamada Rina", 0, 1, 2, 2, 1, 0, 1, "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql1 = 'insert into answers (exec_id, name, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values("abcabcd", "Mai MaiCuong", 0, 1, 2, 0, 1, 0, 1, "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql2 = 'insert into answers (exec_id, name, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values("abcabcd", "Yamada Tarou", 0, 1, 2, 0, 1, 0, 1, "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")';
        # sql3 = "select * from answers";
        
        # sql0 = "ALTER TABLE indexs ADD COLUMN id int NOT NULL auto_increment";
        # cursor.execute(sql0);
        
        # sql1 = 'UPDATE dayLists SET exec_id="abcabcd" WHERE exec_id="0"';
        # cursor.execute(sql0)
        
                # sqlGetDayListId = "select * from indexs";
        sqlGetDayListId = "SELECT MAX(id) AS id FROM indexs";
        cursor.execute(sqlGetDayListId);
        rows = cursor.fetchone()
        print(rows)
        for row in rows:
            dayListId = row
            print(row)
        print(dayListId)
        
        newExecId = '011000010110001001100011' + str(dayListId+1) + '000110010101100110011001110000'; 
        print(newExecId)
        
        title = event['title']
        memo = event['memo']
        
        # cursor.execute(sql1);
        sql2 = 'insert into indexs (exec_id, title, memo) values(%(exec_id)s, %(title)s, %(memo)s)';
        cursor.execute(sql2, {'exec_id' : newExecId, 'title' : title, 'memo' : memo });
        
        dayList = []
        dayListData = event['time']
        
        dayList.append(newExecId)

        for i in range(0, len(dayListData)):
            dayList.append(dayListData[i])
        for i in range(len(dayListData)+2, 22):
            dayList.append("NULL")
        print(dayList)
        
        sql = 'insert into dayLists (exec_id, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)';
            # sql = 'insert into answers (exec_id, name, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18, op19, op20) values(answer[0],answer[1],answer[2],answer[3],answer[4],answer[5],answer[6],answer[7],answer[8],answer[9],answer[10],answer[11],answer[12],answer[13],answer[14],answer[15],answer[16],answer[17],answer[18],answer[19],answer[20],answer[21])';
        
        cursor.execute(sql,dayList);
        
        # logger.info('Here : 1')
        # cursor.execute(sql1)
        # cursor.execute(sql2)
        # conn.commit()
        # cursor.execute("SELECT * FROM indexs")
        # cursor.execute(sql)
        # logger.info('Here : 2')
        # cursor.execute(sql0)
        # logger.info('Here : 3')
        # cursor.execute(sql1)
        # logger.info('Here : 4')
        # cursor.execute(sql2)
        # logger.info('Here : 5')
        # cursor.execute(sql3)
        # logger.info('Here : 6')
        conn.commit()
        # logger.info('Here : 6')
        # cursor.execute(sql3, {'exec_id' : "abcabcd"})
        # logger.info('Here : 7')
        # print(abc)
        # head_rows = cursor.fetchmany(size=2)
        # logger.info('Here : 3')
        # logger.info(cursor)
        # rows = cursor.fetchall()
        # rows = cursor.fetchall()
        # for row in rows:
        #     logger.info(row)
        #     logger.info("hehe")
        
        logger.info('ToHere')
        
        # exec_id = event['exec_id']
        

        
        # sqlGetDayListId = "select dayListId from indexs where exec_id=%(exec_id)s";
        # cursor.execute(sqlGetDayListId, { 'exec_id' : exec_id});
        # rows = cursor.fetchone()
        # for row in rows:
        #     dayListId = row
        # print(dayListId)
        
        # cursor.execute('select locListId from indexs where exec_id="abcabcd"');
        # rows = cursor.fetchone()
        # for row in rows:
        #     locListId = row
        # print(locListId)
        
        # sqlGetDayList = "select * from dayLists where id=%(id)s";
        # cursor.execute(sqlGetDayList, { 'id': dayListId});
        # rows = cursor.fetchall()
        # for row in rows:
        #     dayList = row
        # print(dayList[1])
        # dayListResponse = []
        # for i in range(1, len(dayList)):
        #     logger.info(dayList[i])
        #     if dayList[i] == 'NULL':
        #         break;
        #     dayListResponse.append(dayList[i])
                
        # print(dayListResponse)
            
        # sqlGetLocList = "select * from locLists where id=%(id)s";
        # cursor.execute(sqlGetLocList, { 'id': locListId});
        # rows = cursor.fetchall()
        # for row in rows:
        #     locList = row
        # print(locList[1])
        
        # sqlGetAnswers = 'select * from answers where exec_id="abcabcd"';
        # cursor.execute(sqlGetAnswers);
        # rows = cursor.fetchall()
        # answerData = []
        # for row in rows:
        #     print(row[1])
        #     answers = []
        #     for i in range(2, len(row)):
        #         logger.info(row[i])
        #         if row[i] == 'NULL':
        #             break;
        #         answers.append(row[i])
        #     data = { "name" : row[1], "data" : answers}
        #     print(data)
        #     answerData.append(data)
            
        # response = { "dataList" : dayListResponse, "answersData" : answerData}
        
        # print(answerData)
        # logger.info("DB :OK")
        cursor.close()
        conn.close()
    except:
        logger.error("ERROR: DB ERROR. ahihi")
        # raise DBError()
    return newExecId