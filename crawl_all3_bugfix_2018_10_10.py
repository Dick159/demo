import requests
import json
import OracleDb as db
import time
import random
import pinyin
import  itertools
import configparser
import MongoDb as mongo
import threading
import sys

# author by : hdw

cf=configparser.ConfigParser()
cf.read("config.ini")
#cf.read("config_local.ini")


baseUrl = cf.get("GLOBAL","base_url");
taskDict = {}
tokenParam = None;
batchSize = cf.get("GLOBAL","batch_size");
connection = None;
threadingData = threading.local()


def connMongoDb():
    global cf
    conn = None;
    username = cf.get("MONGO",'username')
    password = cf.get("MONGO",'password') 
    if(username != '' and password != ''):
        conn = mongo.connect(cf.get("MONGO","ip"),int(cf.get("MONGO","port")))
        mongo_auth = conn.admin
        mongo_auth.authenticate(username,  password)
        return conn
    return mongo.connect(cf.get("MONGO","ip"),int(cf.get("MONGO","port")))

# def connOracle():
#     global cf
#     ipaddr = cf.get("DB","ip")
#     username = cf.get("DB","username")
#     password = cf.get("DB","password")
#     oracle_port = cf.get("DB","port")
#     oracle_service = cf.get("DB","oracle_service")
#     connection = db.connect(ipaddr,username,password,oracle_port,oracle_service)
#     cur = connection.cursor();
#     return cur,connection;

def buildTaskGroup(tokenParams):
    global cf
    url = baseUrl + "api/TaskGroup"
    tp = tokenParam[2]+ " " + tokenParam[0]
    headers = {"Authorization": tp}
    response = requests.get(url,headers=headers)
    jsonData = json.loads(response.text)
    for entry in jsonData["data"]:
        t_id = entry["taskGroupId"]
        url =  baseUrl + "api/Task?taskGroupId={}".format(t_id)
        response = requests.get(url,headers=headers)
        taskIdAndName = json.loads(response.text)["data"]
        for data in taskIdAndName:
            s = data["taskName"]
            taskDict[data["taskId"]] = cf.get("TABLE","PRE_FIX") + convertToPinyinLetter(s)

def convertToPinyinLetter(str,offSet=0,length=1):
    result = ''
    for letter in str:
        result += to_pinyin(letter)[offSet:length].upper()
    return result

def getTokenPair():
    global cf
    response = requests.post(url= baseUrl + 'token',data={'username':cf.get("ACCOUNT",'username'),'password':cf.get("ACCOUNT",'password'),'grant_type':'password'},headers={'Content-Type':'application/x-www-form-urlencoded'})
    accessToken = json.loads(response.text)["access_token"]
    refreshToken = json.loads(response.text)["refresh_token"]
    #token_type
    tokenType = json.loads(response.text)["token_type"]
    return [accessToken,refreshToken,tokenType]

def pullData(taskId,offSet,size,tokenParams):
    url,headers = generateRequest(taskId,offSet,size,tokenParams)
    response = requests.get(url,headers=headers)
    return json.loads(response.text)

def generateRequest(taskId,offSet,size,tokenParams):
    url =  baseUrl + "api/alldata/GetDataOfTaskByOffset?taskId={}&offset={}&size={}".format(taskId,offSet,size)
    tp = tokenParam[2]+ " " + tokenParam[0]
    headers = {"Authorization": tp}
    return url,headers;

def getColumns(dataList):
    keys = dataList[0].keys();
    columns = []
    for key in keys:
        columns.append(convertToPinyinLetter(key).upper())
    return columns

def generateId(total):
    return [i for i in range(1,total<<1)]


def to_pinyin(var_str):
    if isinstance(var_str, str):
        if var_str == 'None':
            return ""
        else:
            return pinyin.get(var_str, format='strip', delimiter="")
    else:
        return '类型不对'


# def grow():
#     growMap = [step for step in range(len(threadingData.id_map)+1,len(threadingData.id_map) << 1)]
#     threadingData.id_map = threadingData.id_map + growMap

def generateInsertStatement(columns,values,offset):
    statement = []
    for value in values:
        v = {}
        v["BATCH_OFFSET"] = offset
        index = 0
        for key in value.keys():
            py = convertToPinyinLetter(key)
            if py == "FBSJ":
                v[py] = formatData(value[key])
            else :
                v[py] = value[key]
            index +=1
        statement.append(v)
    return statement

def insertBatch(collection,statement):
    collection.insert(statement);
    

def remove_last_batch(collection,batch_index):
    query = {"BATCH_OFFSET":batch_index}
    collection.delete_many(query)

def getList(cursor):
    ret = []
    for c in cursor:
        ret.append(c)
    return ret


def run(taskId):
        global connection,cf
        threadingData.cur_id = 0
        print("Start running task:{}".format(threading.current_thread().name))
        result = pullData(taskId,0,1,tokenParam)
        #print(result['data']['offset'])
        #Check
        if(int(result['data']['total']) == 0):
            print('no data found for this task:{}'.format(taskId))
            return

        _columns = getColumns(result['data']['dataList'])
        dataInfo = result['data']
        offset = 0
        #generate threadLocal  id map:
        #threadingData.id_map = generateId(int(dataInfo["total"]))
        db = connection[cf.get("MONGO",'db_name')]
        collection = db[taskDict[taskId]]

        for lastest in collection.find().sort([("_id",-1)]).limit(1):
            offset = int(lastest["BATCH_OFFSET"])
            # result = pullData(taskId,offset,batchSize,tokenParam)
            # #avoid dumplicated datas
            # dataInfo = result['data']
            # offset = dataInfo['offset']
            #remove_last_batch(collection,offset)
            db_last_data = collection.find({"BATCH_OFFSET":offset});
            pull_lastest_data = pullData(taskId,offset,batchSize,tokenParam)
            l1 = len(pull_lastest_data['data']['dataList'])
            l2 = len(getList(db_last_data))
            if l1 == l2:
                offset = pull_lastest_data['data']['offset']
            elif l1 > l2:
                diff = l1 - l2
                statement = generateInsertStatement(_columns,pull_lastest_data['data']['dataList'][l1-diff:l1],offset)
                try:
                    insertBatch(collection,statement)
                except Exception as e:
                    print(e)
                offset = pull_lastest_data['data']['offset']



        while dataInfo['restTotal'] > 0:
            result = pullData(taskId,offset,batchSize,tokenParam)
            statement = generateInsertStatement(_columns,result['data']['dataList'],offset)
            try:
                insertBatch(collection,statement)
            except Exception as e:
                print(e)
            dataInfo = result['data']
            offset = dataInfo['offset']
        print('task:{} done,dataCount:{}'.format(threading.current_thread().name,collection.find().count()))

def aggregate(taskDict):
    global cf
    total = 0
    for key in taskDict.keys():
        total += int(connection[cf.get("MONGO",'db_name')][taskDict[key]].find().count())
    print("totalCount:{}".format(total))

def formatData(str):
    if '年' in str or '月' in str or '日' in str:
        ss = str
        ret = ss.replace('年','-').replace('月','-').replace('日','')
        #print(str,ret)
        return ret
    elif '/' in str:
        ss = str
        ret = ss.replace("/","-")
        #print(str,ret)
        return ret
    else:
        return str


if __name__ == '__main__':
    tokenParam = getTokenPair()
    buildTaskGroup(tokenParam)
    connection = connMongoDb()
    for taskId in taskDict.keys():
        t = threading.Thread(target=run, args=(taskId, ), name=taskDict[taskId])
        t.start()
        t.join()
    aggregate(taskDict)
    connection.close()










    # url = baseUrl + "/api/task/RemoveDataByTaskId?taskId=" + 'e2f3062c-9345-4637-9ac4-c08fd5134fe5'
    # tp = tokenParam[2]+ " " + tokenParam[0]
    # headers = {"Authorization": tp}
    # response = requests.post(url,headers=headers)
    # print(response.text)