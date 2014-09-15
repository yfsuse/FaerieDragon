#! /usr/bin/env python
#--*-- coding=utf-8 --*--

import urllib
import json
import pycurl
import StringIO
import ConfigParser


class mysql(object):
    def __init__(self):
        configparser = ConfigParser.ConfigParser()
        configparser.read('/home/jeff/dianyi/FaerieDragon/config/yeahcpa.ini')
        self.login_referer = configparser.get('mysql', 'login_referer')
        self.login_api = configparser.get('mysql', 'login_api')
        self.query_api = configparser.get('mysql', 'query_api')
        self.query_referer = configparser.get('mysql', 'query_referer')
        self.login_data = urllib.urlencode({'email': configparser.get('mysql', 'email'), 'password': configparser.get('mysql', 'password')})

    def get_mysql_data(self, start, end, querystring, timezone=0):

        #登录数据

        crl = pycurl.Curl()
        crl.setopt(pycurl.VERBOSE,1)
        crl.setopt(pycurl.FOLLOWLOCATION, 1)
        crl.setopt(pycurl.MAXREDIRS, 10)
        crl.setopt(pycurl.CONNECTTIMEOUT, 60)
        crl.setopt(pycurl.TIMEOUT, 300)
        crl.setopt(pycurl.HTTPPROXYTUNNEL,1)
        crl.setopt(pycurl.COOKIEFILE, 'cookies/cookie.dat')
        crl.setopt(pycurl.COOKIEJAR, 'cookies/cookie.dat')
        # crl.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36')
        crl.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0')
        crl.setopt(pycurl.REFERER, self.login_referer)

        #模拟登录
        crl.fp = StringIO.StringIO()
        crl.setopt(pycurl.POSTFIELDS, self.login_data)
        crl.setopt(pycurl.URL, self.login_api)
        crl.setopt(crl.WRITEFUNCTION, crl.fp.write)
        crl.perform()
        if not crl.fp.getvalue() == '{"flag":"success","msg":"login is success."}': #登录失败则引发异常
            return 'login mysql report failed'

        #查询数据
        querystring['start'] = start
        querystring['end'] = end
        querystring['timezone'] = timezone

        crl.setopt(pycurl.REFERER, self.query_referer)
        crl.fp = StringIO.StringIO()
        crl.setopt(pycurl.POSTFIELDS, urllib.urlencode(querystring))
        crl.setopt(pycurl.URL, self.query_api)
        crl.setopt(crl.WRITEFUNCTION, crl.fp.write)
        crl.perform()
        try:
            mysql_rsp_list = json.loads(crl.fp.getvalue())['data']['data']
        except KeyError, e:
            return []
        return mysql_rsp_list


if __name__ == '__main__':
    mysqler = mysql()
    print mysqler.get_mysql_data('2014-8-6', '2014-8-7',{"filters[offer_id]":"","statistic[click]": "","statistic[conversion]":"","timezone": 0,"limit": 1000})

