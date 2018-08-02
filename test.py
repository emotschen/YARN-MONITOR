#!/httx/run/python27/bin
#encoding=UTF-8
import requests
import datetime
import time
import heapq
from collections import Counter
from read_config import Properties
import pymysql


#获取JSON串，并存储在字典repo_dicts中
url='http://pc-gzgynn02.100.idc.tf56:19888/ws/v1/history/mapreduce/jobs'
r=requests.get(url)
print("（网页相应状态 返回200：标识成功）Status code :",r.status_code)
response_dic=r.json()
repo_dicts=response_dic['jobs']

#舒适化properties文件，并获取MYSQL数据库
dictProperties=Properties("config/config.properties").getProperties()
db= pymysql.connect(dictProperties['mysql_host'],dictProperties['mysql_user'],dictProperties['mysql_passwd'],dictProperties['mysql_database'],charset="utf8")
cursor=db.cursor()
cursor.execute("SET NAMES utf8")
db.commit()

#读取耗时TOP20指标
xx=repo_dicts['job']
job_id,job_Details,job_costtime,job_name=[],[],[],[]

#读取执行频率top20指标
job_exe_frequency,job_exe_name,job_exe_id=[],[],[]

#获取当前时间
T_1=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
T_2=(datetime.datetime.now()+datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
T_3=(datetime.datetime.now()+datetime.timedelta(days=-3)).strftime("%Y-%m-%d")


#T_1天执行所有任务的计数器
T_Count=0

#统计一天的量
for check in range(len(xx)):
   timeStamp=float(repo_dicts['job'][check]['finishTime']/1000)
   timeArray = time.localtime(timeStamp)
   otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
   if otherStyleTime ==T_1:
       T_Count=T_Count+1
print(T_1+" TOTAL:",T_Count)

#入库yarn_monitor 总表
cursor.execute("insert into yarn_monitor(total,date,importdate) values(%s,%s,%s)",(T_Count,T_1,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

#定义一个字典存储关键数据
arr={}

#时间类型转换，并将job字典中的完成时间和开始时间相减得到时长
for check in range(len(xx)):
    timeStampA=int(repo_dicts['job'][check]['startTime'] / 1000)
    timeArrayA=time.localtime(timeStampA)
    otherStyleTimeA=time.strftime("%Y-%m-%d %H:%M:%S", timeArrayA)
    date_A=datetime.datetime.strptime(otherStyleTimeA,'%Y-%m-%d %H:%M:%S')

    timeStampB=int(repo_dicts['job'][check]['finishTime'] / 1000)
    timeArrayB=time.localtime(timeStampB)
    otherStyleTimeB=time.strftime("%Y-%m-%d %H:%M:%S", timeArrayB)
    #otherStyleTimeCom作为校验 最近3天数据
    otherStyleTimeCom = time.strftime("%Y-%m-%d", timeArrayB)
    date_B=datetime.datetime.strptime(otherStyleTimeB,'%Y-%m-%d %H:%M:%S')

    date_C=date_B-date_A
    date_D=date_C.seconds
    data_E=int(date_D)

#定义指标,这里进行相关指标添加    
    if otherStyleTimeCom in (T_1, T_2, T_3):
        arr.setdefault('name',[]).append(repo_dicts['job'][check]['name'])
        arr.setdefault('Compared',[]).append(date_C)
        arr.setdefault('id',[]).append(repo_dicts['job'][check]['id'])
        arr.setdefault('time',[]).append(otherStyleTimeA)
        arr.setdefault('CostTime',[]).append(data_E)

#2w任务进行排序,筛选出cost TIME top10
expensive = heapq.nlargest(10,arr['Compared'])

#输出TOP 10语句
for check1 in range(len(arr['Compared'])):
    if arr['Compared'][check1]  in expensive:
        job_id.append(arr['id'][check1])
        job_costtime.append(arr['CostTime'][check1])
        job_name.append(arr['name'][check1])

print arr['Compared']



#定义dict存储detail SQL。或者MAPREDUCE.JOB.NAME   arr1[0]=job_id,arr[1]=job_detail,job[2]=map_job_name
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


# 从2W任务中，查询执行频率最多的SQL
sql_top = Counter(arr["name"]).most_common(10)

for t in range(len(sql_top)):
    job_exe_name.append(sql_top[t][0])
    job_exe_frequency.append(sql_top[t][1])
    for check3 in range(len(arr['Compared'])):
        if (sql_top[t][0]==arr['name'][check3]):
            job_exe_id.append(arr['id'][check3])
    cursor.execute("insert into yarn_exe_top(job_id,job_name,job_exec_frequency,importdate) values(%s,%s,%s,%s)",(job_exe_id[t],job_exe_name[t],job_exe_frequency[t],datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

#mysql插入语句
for check2 in range(len(job_id)):
    value=arr1[check2][0]
    cost=job_costtime[check2]
    detail=arr1[check2][1]
    name=job_name[check2]
    #将job_id,costtime,sql_detail,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')入库
    cursor.execute("insert into yarn_job_cost(job_id,job_cost_time,job_name,job_detail,importdate) VALUES(%s,%s,%s,%s,%s)",(value,cost,name,detail,datetime.datetime.now().
strftime('%Y-%m-%d %H:%M:%S')))#提交数据
db.commit()
#关闭数据库会话
db.close()