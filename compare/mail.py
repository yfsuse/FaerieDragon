#!/usr/bin/env python
#  -*- coding=utf-8 -*-

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_mail(receiver, type):
    sender = "jef.yu@yeahmobi.com"
    msg = MIMEMultipart('alternatvie')
    msg['Subject'] = Header("DruidIO Compare Warning: %s" % type,"utf-8") #组装信头
    msg['From'] = r"%s <dev@python.org>" % Header("DruidIO Compare Warning: %s" % type,"utf-8") #使用国际化编码
    msg['To'] = receiver


    try:
        s = smtplib.SMTP('smtp.yeahmobi.com') #登录SMTP服务器,发信
        s.ehlo()
        s.starttls()
        s.login('jef.yu@yeahmobi.com','bmeB500!')
        s.sendmail(sender,receiver,msg.as_string())
    except Exception, e:
        print e


if __name__ == '__main__':
    send_mail('jeff.yu@yeahmobi.com', 'daily')