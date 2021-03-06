
import requests
import time
import pymongo
import datetime

client1 = pymongo.MongoClient('localhost', 27017)
db1 = 'QiHuo'
collection1 = 'qihuo'
database1 = client1[db1][collection1]

client2 = pymongo.MongoClient('10.2.46.149', 1500)
db2 = 'juchao'
collection2 = 'qihuo'
database2 = client2[db2][collection2]

now_year = datetime.datetime.now().year
now_month = datetime.datetime.now().month

m = str(now_month).zfill(2)
if (m=='01'):
    y = now_year - 1
    m = '12'
else:
    y = now_year
    m = str(now_month - 1).zfill(2)

ym = str(y) + str(m)
print(ym)
year = str(y)
mouth = m
url = 'http://www.shfe.com.cn/data/dailydata/%sprice.dat'%ym
time.sleep(5)
req = requests.get(url).json()
req['year'] = year
req['mouth'] = mouth
database1.insert(req)
o_cursor = req['o_cursor']
data_list = []
for o in o_cursor:
    # 品种
    DESCRIPTION = o['DESCRIPTION']
    try:
        # 加权平均价
        AV = o['AV']
        mydict = {'DESCRIPTION': DESCRIPTION, 'AV': AV}
        data_list.append(mydict)
    except:
        try:
            # 月度参考价
            SETAV = o['SETAV']
            mydict = {'DESCRIPTION': DESCRIPTION, 'SETAV': SETAV}
            data_list.append(mydict)
        except:
            print(ym, DESCRIPTION)
            with open('fail.text', 'w+a') as f:
                f.write(ym + ' ' + DESCRIPTION)
mytime = {'year': year,'mouth': mouth}
save_dict = {'time': mytime, 'data': data_list}
database2.insert(save_dict)