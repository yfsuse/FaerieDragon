#! /usr/bin/env python
#--*-- coding=utf-8 --*--


"""
    get mysql data and druid data by query
"""

from __future__ import division
from get_druid import druid
from get_mysql import mysql
from datetime import date, timedelta
import numpy as np
import logging
import json
import ConfigParser
from mail import send_mail


# global variable
DRUID_QUERY = '/home/jeff/dianyi/FaerieDragon/extern/druidquery.list'
MYSQL_QUERY = '/home/jeff/dianyi/FaerieDragon/extern/mysqlquery.list'

class Log(object):

    def __init__(self, logname):
        self.logger = logging.getLogger('Compare Debug')
        self.logger.setLevel(logging.INFO)
        self._set_log(logname)

    def _get_logger(self):
        return self.logger

    def _set_log(self, logname):
        # create a handler for echo to file
        fh = logging.FileHandler('/home/jeff/dianyi/FaerieDragon/log/%s.log' % logname)
        fh.setLevel(logging.DEBUG)
        # create a handler for echo to console
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # define format of log
        formatter = logging.Formatter('%(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add handler to logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


class Control(object):
    def __init__(self):
        configparser = ConfigParser.ConfigParser()
        configparser.read('/home/jeff/dianyi/FaerieDragon/config/yeahcpa.ini')
        self.receiver = configparser.get('mail', 'receiver')
        try:
            self.click_max_offset = int(configparser.get('mail', 'click_max_offset'))
            self.conv_max_offset = int(configparser.get('mail', 'conv_max_offset'))
        except ValueError:
            self.click_max_offset, self.conv_max_offset = 50, 50
        fobj = open(DRUID_QUERY, 'r')
        self.druid_list = fobj.readlines()
        fobj.close()

        fobj = open(MYSQL_QUERY, 'r')
        self.mysql_list = fobj.readlines()
        fobj.close()
        self.keytable = {'0': 0,
                         '1': 2,
                         '2': 1,
                         '3': 3,
                         '4': 2,
                         '5': 1}

        self.deltable = {
                         '1':[1, 2],
                         '2':[1],
                         '3':[2, 3],
                         '4':[2]}

        self.am_json = {u'Kay Zhao':44,
                        u'Maria Xin':24,
                        u'sunny yang':100017,
                        u'Lena Liu':8,
                        u'Sara Xu':19,
                        u'Vivian Chang':38,
                        u'Darcy Yuan':13281,
                        u'Lexie Hu':46,
                        u'Alice Yang':42,
                        u'Andy Sun':34,
                        u'Alan Xu':54,
                        u'Grace Lee':2,
                        u'owen Liu':56,
                        u'Steven Wang':26,
                        u'unknown':0}

        self.druider = druid()
        self.mysqler = mysql()
        self.logname = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')
        self.logger = Log(self.logname)._get_logger()

    def get_data(self):
        # get data
        time_end = date.today()
        time_start = time_end + timedelta(days=-1)
        str_time_start, str_time_end = time_start.strftime('%Y-%m-%d'), time_end.strftime('%Y-%m-%d')
        for timezone in [8]:
            for index in range(6):
                mysql_data = self.mysqler.get_mysql_data(str_time_start, str_time_end, json.loads(self.mysql_list[index].strip('\n')), timezone = 0)
                druid_data = self.druider.get_druid_data(str_time_start, str_time_end, self.druid_list[index].strip('\n'), timezone = 0)
                delindex_list = self.deltable.get(str(index), '')
                if not delindex_list:
                    pass
                else:
                    for delindex in delindex_list:
                        for data in mysql_data:
                            del data[delindex]
                self.check(mysql_data, druid_data, index, timezone)

    def check(self, mysql_data, druid_data, index, timezone=0):
        key_separator = self.keytable.get(str(index))
        mysql_data_json = {}
        mysql_list_key = []

        for datas in mysql_data[1:]:
            list_key = [unicode(data) for data in datas[:key_separator]]
            mysql_list_key.append(list_key)
            str_key = str(list_key)
            value = datas[key_separator:]
            mysql_data_json[str_key] = value

        druid_data_json = {}
        for datas in druid_data[1:]:
            key = str([unicode(data) for data in datas[:key_separator]])
            value = datas[key_separator:]
            druid_data_json[key] = value

        self.logger.info("----------------------------------------group : %s  filters: no fileter---------------------------------------" % mysql_data[0][:key_separator])
        for mysql_key in mysql_list_key:
            druid_key = mysql_key[:]
            if index == 3:
                druid_key[0] = unicode(self.am_json.get(mysql_key[0]))
            elif index == 5:
                druid_key[0] = unicode(self.am_json.get(mysql_key[0]))
            mysql_value, druid_value = mysql_data_json.get(str(mysql_key), [0, 0]), druid_data_json.get(str(druid_key), [0, 0])
            if not mysql_value == druid_value:
                mysql_value_n, druid_value_n = np.array(mysql_value), np.array(druid_value)
                click_offset = abs((mysql_value_n - druid_value_n)[0])
                conv_offset = abs((mysql_value_n - druid_value_n)[1])
                if mysql_value[0] == 0:
                    click_offset_percentage = click_offset
                else:
                    click_offset_percentage = click_offset / mysql_value[0]
                if mysql_value[1] == 0:
                    conv_offset_percentage = conv_offset
                else:
                    conv_offset_percentage = conv_offset / mysql_value[1]
                runinfo = "{'group value': %35s, 'mysql_data': %15s, 'druid_data': %15s, 'click_offset':%5d, 'click_offset_percentage':%d%%, 'conv_offset':%5d, 'conv_offset_percentage':%d%%}" % (druid_key, str(mysql_value), str(druid_value), click_offset, 100 * click_offset_percentage, conv_offset, 100 * conv_offset_percentage)
                self.logger.info(runinfo)
                if click_offset > self.click_max_offset or conv_offset > self.conv_max_offset:
                    mailinfo = "Mysql & DruidIO Cmpare: {'group value': %s, 'mysql_data': %s, 'druid_data': %s, 'click_offset':%d, 'click_offset_percentage':%d%%, 'conv_offset':%d, 'conv_offset_percentage':%d%%}" % (druid_key, str(mysql_value), str(druid_value), click_offset, 100 * click_offset_percentage, conv_offset, 100 * conv_offset_percentage)
                    send_mail(self.receiver, mailinfo)

if __name__ == '__main__':
    controler = Control()
    controler.get_data()
