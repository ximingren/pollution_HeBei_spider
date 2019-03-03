import datetime
import smtplib
from datetime import date
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from enum import Enum

import pandas as pd


def send_mail():
    smtp_server = 'smtp.163.com'
    from_addr = 'ximingren32@163.com'
    password = 'Ximingren923162'
    to_addr = 'yanlunka@163.com'
    msg = MIMEMultipart()
    msg['Subject'] = Header('xlsx文件')
    msg['From'] = from_addr
    msg['To'] = to_addr
    mime = MIMEApplication(open('result3.xlsx', 'rb').read())
    mime.add_header('Content-Disposition', 'attachment', filename='石家庄钢铁有限责任公司_国控.xlsx')
    # 添加到MIMEMultipart:
    msg.attach(mime)
    server = smtplib.SMTP(smtp_server, 25)
    try:
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
    except Exception as e:
        print('发送失败', to_addr, '出现异常', e, to_addr)
    else:
        print('发送成功', to_addr)
    finally:
        server.quit()


def read_data():
    data = pd.read_excel('TargetCheckPoint.xlsx',index_col=0)
    needCom = data.index
    print(len(needCom.dropna()))
    # for i in data['石家庄钢铁有限责任公司_国控']:
    #     print(i == i)
    needCom = list(data.iloc[:, 0])
    needCom.extend(data.columns)


def get_date():
    print(datetime.datetime.now().date())
#要看返回的是什么

def test_enum():
    Contrast = Enum('input', ('1', '2'))
    d = Contrast['1']
    for i in Contrast:
        print(i)
    print(d)
    print('input.1' in Contrast)

def test_date():
    a=datetime.datetime.now()
    b=datetime.datetime.strptime('2018-12-01 08:00:00','%Y-%m-%d %H:%M:%S')
    c=(a-b).seconds
    print(c/3600)


if __name__ == '__main__':
    read_data()
