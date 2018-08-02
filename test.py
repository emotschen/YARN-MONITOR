#!/httx/run/python27/bin
#encoding=UTF-8
import requests
import datetime
import time
import heapq
from collections import Counter
from read_config import Properties
import pymysql


#��ȡJSON�������洢���ֵ�repo_dicts��
url='http://pc-gzgynn02.100.idc.tf56:19888/ws/v1/history/mapreduce/jobs'
r=requests.get(url)
print("����ҳ��Ӧ״̬ ����200����ʶ�ɹ���Status code :",r.status_code)
response_dic=r.json()
repo_dicts=response_dic['jobs']

#���ʻ�properties�ļ�������ȡMYSQL���ݿ�
dictProperties=Properties("config/config.properties").getProperties()
db= pymysql.connect(dictProperties['mysql_host'],dictProperties['mysql_user'],dictProperties['mysql_passwd'],dictProperties['mysql_database'],charset="utf8")
cursor=db.cursor()
cursor.execute("SET NAMES utf8")
db.commit()

#��ȡ��ʱTOP20ָ��
xx=repo_dicts['job']
job_id,job_Details,job_costtime,job_name=[],[],[],[]

#��ȡִ��Ƶ��top20ָ��
job_exe_frequency,job_exe_name,job_exe_id=[],[],[]

#��ȡ��ǰʱ��
T_1=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
T_2=(datetime.datetime.now()+datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
T_3=(datetime.datetime.now()+datetime.timedelta(days=-3)).strftime("%Y-%m-%d")


#T_1��ִ����������ļ�����
T_Count=0

#ͳ��һ�����
for check in range(len(xx)):
   timeStamp=float(repo_dicts['job'][check]['finishTime']/1000)
   timeArray = time.localtime(timeStamp)
   otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
   if otherStyleTime ==T_1:
       T_Count=T_Count+1
print(T_1+" TOTAL:",T_Count)

#���yarn_monitor �ܱ�
cursor.execute("insert into yarn_monitor(total,date,importdate) values(%s,%s,%s)",(T_Count,T_1,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

#����һ���ֵ�洢�ؼ�����
arr={}

#ʱ������ת��������job�ֵ��е����ʱ��Ϳ�ʼʱ������õ�ʱ��
for check in range(len(xx)):
    timeStampA=int(repo_dicts['job'][check]['startTime'] / 1000)
    timeArrayA=time.localtime(timeStampA)
    otherStyleTimeA=time.strftime("%Y-%m-%d %H:%M:%S", timeArrayA)
    date_A=datetime.datetime.strptime(otherStyleTimeA,'%Y-%m-%d %H:%M:%S')

    timeStampB=int(repo_dicts['job'][check]['finishTime'] / 1000)
    timeArrayB=time.localtime(timeStampB)
    otherStyleTimeB=time.strftime("%Y-%m-%d %H:%M:%S", timeArrayB)
    #otherStyleTimeCom��ΪУ�� ���3������
    otherStyleTimeCom = time.strftime("%Y-%m-%d", timeArrayB)
    date_B=datetime.datetime.strptime(otherStyleTimeB,'%Y-%m-%d %H:%M:%S')

    date_C=date_B-date_A
    date_D=date_C.seconds
    data_E=int(date_D)

#����ָ��,����������ָ�����    
    if otherStyleTimeCom in (T_1, T_2, T_3):
        arr.setdefault('name',[]).append(repo_dicts['job'][check]['name'])
        arr.setdefault('Compared',[]).append(date_C)
        arr.setdefault('id',[]).append(repo_dicts['job'][check]['id'])
        arr.setdefault('time',[]).append(otherStyleTimeA)
        arr.setdefault('CostTime',[]).append(data_E)

#2w�����������,ɸѡ��cost TIME top10
expensive = heapq.nlargest(10,arr['Compared'])

#���TOP 10���
for check1 in range(len(arr['Compared'])):
    if arr['Compared'][check1]  in expensive:
        job_id.append(arr['id'][check1])
        job_costtime.append(arr['CostTime'][check1])
        job_name.append(arr['name'][check1])

print arr['Compared']



#����dict�洢detail SQL������MAPREDUCE.JOB.NAME   arr1[0]=job_id,arr[1]=job_detail,job[2]=map_job_name
arr1 = [[0 for col in range(3)] for row in range(10)]

for job_id_ck in range(len(job_id)):
    joburl='http://pc-gzgynn02.100.idc.tf56:19888/ws/v1/history/mapreduce/jobs/'+job_id[job_id_ck]+'/conf'
    job_r=requests.get(joburl)
    job_dic=job_r.json()
    job_dics=job_dic['conf']
    job_property=job_dics['property']
    arr1[job_id_ck][0]=job_id[job_id_ck]
    for hive_check in range(len(job_property)):
        job_conf=job_property[hive_check]
        if (job_conf['name']=='hive.query.string' ):
            arr1[job_id_ck][1]=job_conf['value']
        elif(job_conf['name']=='mapreduce.job.name'):
            arr1[job_id_ck][2]=job_conf['value']


# ��2W�����У���ѯִ��Ƶ������SQL
sql_top = Counter(arr["name"]).most_common(10)

for t in range(len(sql_top)):
    job_exe_name.append(sql_top[t][0])
    job_exe_frequency.append(sql_top[t][1])
    for check3 in range(len(arr['Compared'])):
        if (sql_top[t][0]==arr['name'][check3]):
            job_exe_id.append(arr['id'][check3])
    cursor.execute("insert into yarn_exe_top(job_id,job_name,job_exec_frequency,importdate) values(%s,%s,%s,%s)",(job_exe_id[t],job_exe_name[t],job_exe_frequency[t],datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

#mysql�������
for check2 in range(len(job_id)):
    value=arr1[check2][0]
    cost=job_costtime[check2]
    detail=arr1[check2][1]
    name=job_name[check2]
    #��job_id,costtime,sql_detail,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')���
    cursor.execute("insert into yarn_job_cost(job_id,job_cost_time,job_name,job_detail,importdate) VALUES(%s,%s,%s,%s,%s)",(value,cost,name,detail,datetime.datetime.now().
strftime('%Y-%m-%d %H:%M:%S')))#�ύ����
db.commit()
#�ر����ݿ�Ự
db.close()