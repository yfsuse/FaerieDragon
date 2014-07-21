#! /usr/bin/env python
#--*-- coding=utf-8 --*--


"""
    get druid data from query
"""

import json
import urllib
import urllib2
from datetime import date, timedelta
import time
from get_topquery import query

# global variable
DRUID_QUERY = '/home/jeff/dianyi/FaerieDragon/extern/druidquery.list'
MYSQL_QUERY = '/home/jeff/dianyi/FaerieDragon/extern/mysqlquery.list'

class druid(object):
    def __init__(self):
        self.query_url = 'http://resin-yeahmobi-214401877.us-east-1.elb.amazonaws.com:18080/report/report?'
        self.query_template = '{"settings":{"report_id":"report_id-x008","return_format":"json","time":{"start":%d,"end":%d,"timezone":0},"data_source":"ymds_druid_datasource","pagination":{"size":50,"page":0}},"filters":{"$and":{"offer_id":{"$eq":13232}}},"data":["click", "conversion"],"group":%s}'

    @staticmethod
    def convert_to_timestamp(start, end): #unix timestamp
        return int(time.mktime(time.strptime(start, '%Y-%m-%d'))) + 28800, int(time.mktime(time.strptime(end, '%Y-%m-%d'))) + 28800

    def get_druid_data(self, start, end, groupdata, timezone=0):
        # create query string

        unix_start, unix_end = druid.convert_to_timestamp(start, end)
        runtimezone = '"timezone":timezone'
        self.query_template.replace('"timezone":0', runtimezone)

        #replace "'" to '"'
        if groupdata == 'NULL':
            groupdata = []
        else:
            groupdata = str(groupdata).replace("u'", '"')
            groupdata = str(groupdata).replace("'", '"')

        query = self.query_template %  (unix_start, unix_end, groupdata)

        # send query string
        postdata = urllib.urlencode({'report_param': query})
        rsp = urllib2.build_opener().open(urllib2.Request(self.query_url, postdata)).read()
        try:
            mysql_rsp_list = json.loads(rsp)['data']['data']
        except ValueError:
            return 'no data return'
        return mysql_rsp_list

if __name__ == '__main__':
    fobj = open(DRUID_QUERY, 'r')
    druid_list = fobj.readlines()
    fobj.close()

    druider = druid()
    for query in druid_list:
        query = query.strip('\n')
        print druider.get_druid_data('2014-7-10', '2014-7-11', query)



