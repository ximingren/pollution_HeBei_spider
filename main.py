import datetime
import json
import os
import smtplib
import time
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

import pandas as pd
import requests

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
}
total_num = None
nowDate = str(datetime.datetime.now().date())
tip_start = nowDate + ' 00:00:00'
tip_to = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
s_startTime = s_fromTime = datetime.datetime.strptime(str((datetime.datetime.now().date()-datetime.timedelta(days=1)))+' 08:00:00', '%Y-%m-%d %H:%M:%S')
s_toTime = s_endTime = datetime.datetime.strptime(nowDate + ' 07:00:00', '%Y-%m-%d %H:%M:%S')
c_fromTime =s_startTime-datetime.timedelta(days=1)
c_toTime=s_toTime-datetime.timedelta(days=1)
s_date = str(s_toTime.date())
c_day = 1
sleep = 0.1
contrast='1'
allShow = True
# flag = '钢'
resultFile = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '_result.xlsx'
indexUrl = 'http://110.249.223.75:9090/onlinemonitor/index.html'
loginUrl = 'http://110.249.223.75:9090/onlinemonitor/mon/sysuser/login'
portCodeUrl = 'http://110.249.223.75:9090/onlinemonitor/mon/portInfo/get'
monitorUrl = 'http://110.249.223.75:9090/onlinemonitor//mon/portInfo/findSelectListByPsIdPortTypeCode'
pollutionUrl = 'http://110.249.223.75:9090/onlinemonitor//mon/portInfo/search/pollutantCodeById'
headerUrl = 'http://110.249.223.75:9090/onlinemonitor/mon/dataQueryHb/search/header'
treeUrl = 'http://110.249.223.75:9090/onlinemonitor/mon/psBaseInfo/list/search/administration/tree'
searchUrl = 'http://110.249.223.75:9090/onlinemonitor/mon/dataQueryHb/page/search'
errorDict = []
c_hour = ((c_toTime - c_fromTime).days * 24) + int((c_toTime - c_fromTime).seconds / 3600) + 1
s_hour = ((s_toTime - s_fromTime).days * 24) + int(((s_toTime - s_fromTime).seconds) / 3600) + 1

def login():
    """
    登录
    :return:
    """
    session.get(indexUrl, timeout=180)
    session.post(loginUrl, data=json.dumps(
        {"suLoginid": "public", "suPasswd": "123", "sdId": "d211c9993e4844cebe510db6d4a7bd7d"}), headers=headers,
                 timeout=180)


def get_portCode(com):
    res = session.post(portCodeUrl,
                       data=json.dumps({"porttypeCode": 1, "psid": com['id']}), headers=headers, timeout=180)
    portCode = 2  # 排口类型
    if res.json()['success']:
        if res.json()['data']:
            portCode = 1
    res = session.post(portCodeUrl,
                       data=json.dumps({"porttypeCode": 2, "psid": com['id']}), headers=headers, timeout=180)
    if res.json()['success']:
        if res.json()['data']:
            portCode = 2
    return portCode


def get_monitorCode(pollutionType, com):
    monitorpointDict = {}
    res = session.post(
        monitorUrl,
        data=json.dumps(
            {"portTypeCode": pollutionType, "psId": com['id'], "startTime": str(s_startTime),
             "endTime": str(s_endTime)}), headers=headers, timeout=180)
    for i in res.json()['data']:
        name = com['name'].strip()
        condition, _ = judge(needData.loc[name, :].values, i['name'])
        if condition:  # 对监控点进行判断
            monitorpointtype_code = i['id']  # 监控点
            monitorpoint = i['name']  # 监控点
            monitorpointDict[monitorpointtype_code] = monitorpoint
    return monitorpointDict


def get_pollutionCode(portCode, com):
    res = session.post(
        pollutionUrl,
        data=json.dumps({"portCode": portCode, "psId": com['id']}), headers=headers, timeout=180)
    if len(res.json()['data']) == 0:
        pollutionCode = '0'
    else:
        pollutionCode = res.json()['data'][0]['id']
    return pollutionCode


def get_header(pollutionType, portCode, com, monitorpointtype_code):
    resHeader = session.post(headerUrl,
                             data=json.dumps({"colStr": pollutionType, "periodType": "2061",
                                              "pollutionType": portCode, "psId": com['id'],
                                              "outletNo": monitorpointtype_code, "choose": "1"}),  # outletNO是监控点,
                             # perodType是数据类型,pollutionType是排口类型,colStr是污染物,choose是查询类型
                             headers=headers, timeout=180)
    header = []
    for k in resHeader.json()['data']:
        if allShow:
            if k['checked']:
                header.append(k['id'])
        else:
            header.append(k['id'])
    header = ','.join(header)
    return header


def get_field(columnsRes, field):
    columns = {}
    for k in columnsRes:
        if k['children']:
            for c in k['children']:
                columns[c['id']] = k['name'] + "--" + c['name']
                # field.append(k['name'] + "--" + c['name'])
        else:
            columns[k['id']] = k['name']
            # field.append(k['name'])
    return columns


def s_judge_time(i):
    searchTime = datetime.datetime.strptime(i['date'].strip(), '%Y-%m-%d %H:%M:%S')
    if searchTime >= s_fromTime and searchTime <= s_toTime:
        return True
    return False


def c_judge_time(g):
    searchTime = datetime.datetime.strptime(g['date'].strip(), '%Y-%m-%d %H:%M:%S')
    if searchTime >= c_fromTime and searchTime <= c_toTime:
        return True
    return False


def get_contrast(pollutionType, portCode, com, monitorpointtype_code, monitorpoint, header, start, columns):
    recordsTotal = 100
    now = 0
    c_totalTable = []
    c_operate_rate=0
    while (recordsTotal > now):
        print('正在获取对比数据 %s 的第%s页' % (com['name'] + " " + monitorpoint, start))
        contrastData = session.post(
            searchUrl,
            data=json.dumps({"length": 25,
                             "search": {"psId": com['id'], "pollutionType": portCode,
                                        "outletNo": monitorpointtype_code, "colStr": pollutionType,
                                        "periodType": "2061", "fromTime": str(c_fromTime),
                                        "toTime": str(c_toTime),
                                        "header": header, "choose": "1",
                                        "data_source": "1"}, "start": start}),
            headers=headers, timeout=180)
        recordsTotal = contrastData.json()['recordsTotal']
        now = now + 25
        for g in contrastData.json()['data']:
            condition = c_judge_time(g)
            if condition:
                temp = {}
                for k, v in g.items():
                    if k in columns.keys():
                        if columns[k] == '是否停运':
                            temp[c_date + '是否停运'] = v
                            if v.strip()=='-':
                                c_operate_rate = c_operate_rate + 1
                        elif columns[k] == '监测时间':
                            temp[c_date + '监测时间'] = v
                        else:
                            temp[columns[k]] = v
                            temp['add_flag'] = True
                c_totalTable.append(temp)
                # for i in tempTable:
                #     if datetime.datetime.st2018-rptime(temp[str(c_fromTime.date()) + '监测时间'],
                #                                   '%Y-%m-%d %H:%M:%S') == datetime.datetime.strptime(
                #         i[str(s_date) + '监测时间'], '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=c_day):
                #         i.update(temp)
                # for i in tempTable:
                #     if temp[str(c_fromTime.date()) + '监测时间'][10:] == i[nowDate + '监测时间'][10:]:
                #         if temp[str(c_fromTime.date()) + '是否停运'] == i[nowDate + '是否停运']:
                #             temp['是否有变动'] = '-'
                #             temp.update(i)
                #         else:
                #             temp['是否有变动'] = '是'
                #             temp.update(i)
    return c_totalTable, c_operate_rate


def get_table(pollutionType, portCode, com, monitorpointtype_code, header, city, area, monitorpoint):
    start = 0
    now = 0
    s_operate_rate = 0
    recordsTotal = 100
    s_totalTable = []
    result_table = []
    while (recordsTotal > now):
        s_table = []
        start = start + 1
        print('正在获取 %s 的第%s页' % (com['name'] + " " + monitorpoint, start))
        searchData = session.post(
            searchUrl,
            data=json.dumps({"length": 25,
                             "search": {"psId": com['id'], "pollutionType": portCode,
                                        "outletNo": monitorpointtype_code, "colStr": pollutionType,
                                        "periodType": "2061", "fromTime": str(s_fromTime),
                                        "toTime": str(s_toTime),
                                        "header": header, "choose": "1",
                                        "data_source": "1"}, "start": start}),
            headers=headers, timeout=180)

        recordsTotal = searchData.json()['recordsTotal']
        now = now + 25
        columnsRes = searchData.json()['columns']
        global columns
        columns = get_field(columnsRes, field)
        for i in searchData.json()['data']:
            condition = s_judge_time(i)
            if condition:
                temp = {}
                temp['城市'] = city
                temp['区域'] = area
                temp['公司'] = com['name']
                temp['监控口'] = monitorpoint
                for k, v in i.items():
                    if k in columns.keys():
                        if columns[k] == '是否停运':
                            temp[s_date + '是否停运'] = v
                            if v.strip()=='-':
                                s_operate_rate = s_operate_rate + 1
                        elif columns[k] == '监测时间':
                            temp[s_date + '监测时间'] = v
                        else:
                            temp[columns[k]] = v
                s_totalTable.append(temp)
    if contrast == '1':
        c_totalTable, c_operate_rate = get_contrast(pollutionType, portCode, com, monitorpointtype_code,
                                                    monitorpoint, header,
                                                    1,
                                                    columns)
        for c in c_totalTable:
            for s in s_totalTable:
                if datetime.datetime.strptime(c[c_date + '监测时间'],
                                              '%Y-%m-%d %H:%M:%S') == datetime.datetime.strptime(
                    s[str(s_date) + '监测时间'], '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=c_day):
                    s.update(c)
        global c_operate_rate
        c_operate_rate = c_hour if c_operate_rate > c_hour else c_operate_rate
    s_operate_rate = s_hour if s_operate_rate > s_hour else s_operate_rate
    for k in s_totalTable:
        if 'add_flag' in k.keys():
            c_rate=float(c_operate_rate / c_hour)
            s_rate=float(s_operate_rate / s_hour)
            k[c_date + '开工率'] = str((c_rate) * 100)[0:6] + '%'
            k[s_date + '开工率'] = str((s_rate) * 100)[0:6] + '%'
            if c_rate > s_rate:
                k['增产/减产/不变'] = '减产'
            elif c_rate < s_rate:
                k['增产/减产/不变'] = '增产'
            else:
                k['增产/减产/不变'] = '不变'
            k[str(s_date) + '监测时间'] = str(s_fromTime) + '---' + str(s_toTime)
            k[c_date + '监测时间'] = str(c_fromTime) + '---' + str(c_toTime)
            result_table.append(k)
            break
    return result_table


def save_data(table, fileName, monitorpoint, num):
    try:
        temp = {}
        for i in table:
            for k, v in i.items():
                temp.setdefault(k, []).append(v)
        data = pd.DataFrame.from_dict(temp)
        if os.path.exists(resultFile):
            row = pd.read_excel(resultFile)
            data = row.append(data)
        data.to_excel(resultFile, index=False, columns=field)
    except Exception as e:
        print('保存数据错误', e)
    else:
        print('保存 第%d个数据 %s 成功' % (num, str(fileName) + " " + monitorpoint))


def get_data(city, area, com, num=0):
    try:
        portCode = get_portCode(com)
        monitorpointDict = get_monitorCode(portCode, com)  # 这里可能有点问题
        for monitorpointtype_code, monitorpoint in monitorpointDict.items():
            pollutionType = get_pollutionCode(monitorpointtype_code, com)
            header = get_header(pollutionType, portCode, com, monitorpointtype_code)
            table = get_table(pollutionType, portCode, com, monitorpointtype_code, header, city['name'],
                              area['name'],
                              monitorpoint)
            save_data(table, com['name'], monitorpoint, num)
    except Exception as e:
        print(e)
        error = {}
        error['id'] = com['id']
        error['name'] = com['name']
        error['city'] = city
        error['area'] = area
        print('添加爬取失败的公司%s,稍后接着爬取' % (com['id']))
        errorDict.append(error)


def judge(list, name):
    if name in list:
        return True,name
    # for i in list:
    #     if i == i:
    #         i = i.strip()  # 要去除两边的空格
    #         name = name.strip()
    #         if i in name or name in i:
    #             return True, i
    return False, name


def crawl_main():
    try:
        num = 0
        tree = session.post(treeUrl,
                            data=json.dumps({}),
                            headers=headers, timeout=180)
        for city in tree.json()['data']:
            areaTree = session.post(treeUrl,
                                    data=json.dumps({"id": city['id']}), headers=headers, timeout=180)
            for area in areaTree.json()['data']:
                areaId = area['id']
                print(city['name'], area['name'], areaId)
                comTree = session.post(treeUrl,
                                       data=json.dumps({"id": areaId}), headers=headers, timeout=180)
                if comTree.json()['data']:
                    for com in comTree.json()['data']:
                        # if flag in com['name'] and judge(com['name']):
                        condition, name = judge(needCom, com['name'].strip())
                        if condition:
                            num = num + 1
                            com['name'] = name
                            print('符合条件 ', com['name'], com['id'])
                            get_data(city, area, com, num)
                # if flag in area['name'] and judge(area['name']):
                try:
                    int(area['id'])
                except Exception:
                    condition1 = True
                else:
                    condition1 = False
                condition, name = judge(needCom, area['name'].strip())
                if condition and condition1:
                    num = num + 1
                    print('符合条件 ', area['name'], area['id'])
                    com = {}
                    com['id'] = areaId
                    com['name'] = name
                    get_data(city, area, com, num)
                time.sleep(sleep)
                if num == total_num+1:
                    return
    except Exception as e:
        print('发生异常,请检查网络是否正常', e)


def send_mail():
    msg = MIMEMultipart()
    msg['subject'] = Header(str(datetime.datetime.now().date()) + '河北钢厂停开工数据', 'utf8')
    msg['From'] = from_addr
    msg['To'] = to_addr
    mime = MIMEApplication(open(resultFile, 'rb').read())
    mime.add_header('Content-Disposition', 'attachment', filename=resultFile)
    msg.attach(mime)
    server = smtplib.SMTP(smtp_server, 25)
    try:
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
    except Exception as e:
        print('发送邮箱失败', to_addr, '出现异常', e)
    else:
        print('发送邮箱成功', to_addr)
    finally:
        server.quit()


def get_input():
    global s_fromTime, s_startTime, s_toTime, s_endTime, c_fromTime, c_toTime, contrast, smtp_server, from_addr, password, to_addr, sleep, c_hour, s_hour, s_date, c_day,c_date
    while (True):
        try:
            input1 = input('输入开始时间(eg ' + str(tip_start) + ')\n')
            s_fromTime = s_startTime = datetime.datetime.strptime(input1, '%Y-%m-%d %H:%M:%S')
            input2 = input('输入结束时间(eg ' + str(tip_to) + ')\n')
            s_toTime = s_endTime = datetime.datetime.strptime(input2, '%Y-%m-%d %H:%M:%S')
            s_date = str(s_toTime.date())
            input4 = input('是否取对比数据(是输入1,否输入0)\n')
            if input4 != '1' and input4 != '0':
                raise TypeError
            else:
                contrast = input4
            c_fromTime = c_toTime =c_date= None
            if contrast == '1':
                input7 = input('是否要输入特定对比日期(是输入1,取昨天数据进行对比输入0)\n')
                if input7 == '1':
                    input5 = input('请输入对比起始时间\n')
                    c_fromTime = datetime.datetime.strptime(input5, '%Y-%m-%d %H:%M:%S')
                    c_day = (s_fromTime - c_fromTime).days
                    input6 = input('请输入对比结束时间\n')
                    c_toTime = datetime.datetime.strptime(input6, '%Y-%m-%d %H:%M:%S')
                    c_date=str(c_toTime.date())
                elif input7 == '0':
                    c_fromTime = datetime.datetime.strptime(str(s_fromTime), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(
                        days=1)
                    c_toTime = datetime.datetime.strptime(str(s_toTime), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(
                        days=1)
                    c_date = str(c_toTime.date())
                else:
                    raise Exception
                c_hour = ((c_toTime - c_fromTime).days * 24) + int((c_toTime - c_fromTime).seconds / 3600) + 1
            smtp_server = input('输入smtp服务器地址\n')
            from_addr = input('发件人邮箱\n')
            password = input('发件人邮箱授权码\n')
            to_addr = input('收件人邮箱\n')
            input3 = input('输入请求间隔时间(默认为1)\n')
            if input3:
                sleep = float(input3)
            s_hour = ((s_toTime - s_fromTime).days * 24) + int(((s_toTime - s_fromTime).seconds) / 3600) + 1
        except Exception as e:
            print('输入错误,请重新输入')
        else:
            break


def set_field():
    global field
    field = []
    field.append('城市')
    field.append('区域')
    field.append('公司')
    field.append('监控口')
    if contrast == '1':
        field.append(s_date + '监测时间')
        # field.append(s_date + '是否停运')
        field.append(s_date + '开工率')
        field.append(c_date + '监测时间')
        # field.append(str(c_toTime.date()) + '是否停运')
        field.append(c_date + '开工率')
        field.append('增产/减产/不变')
    else:
        field.append(s_date + '监测时间')
        # field.append(s_date + '是否停运')
        field.append(s_date + '开工率')


if __name__ == '__main__':
    needData = pd.read_excel('TargetCheckPoint.xlsx', index_col=0)
    needCom = needData.index.dropna()
    total_num = len(needCom)
    session = requests.session()
    get_input()

    set_field()
    login()
    crawl_main()
    print('爬取完成\n')
    if errorDict:
        print('休眠30s后继续爬取失败的数据')
        time.sleep(30)
        for i in errorDict:
            get_data(i['city'], i['area'], {'name': i['name'], 'id': i['id']})
    send_mail()
