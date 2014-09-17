#! /usr/bin/env python



from datetime import datetime, timedelta
import urllib, urllib2
import os
import json
from central import Log
import ConfigParser
from common import BASEDIR


BIG_SMALL_LIST = BASEDIR + 'extern/big_small_table.list'

def get_query():
    fobj = open(BIG_SMALL_LIST,'r')
    querys = fobj.readlines()
    fobj.close()
    return querys

class big_small(object):
    def __init__(self):
        configparser = ConfigParser.ConfigParser()
        configparser.read(BASEDIR + 'config/yeahcpa.ini')
        querys = get_query()
        self.small, self.big = querys[0], querys[1]
        self.starttime = (datetime.now()).strftime('%Y-%m-%d %H')
        self.endtime = (datetime.now() + timedelta(hours=-1)).strftime('%Y-%m-%d %H')
        self.unixend = int(os.popen("date -d '%s' +%%s" % self.starttime).read().strip())
        self.unixstart = int(os.popen("date -d '%s' +%%s" % self.endtime).read().strip())
        self.small = self.small % (self.unixstart, self.unixend)
        self.big = self.big % (self.unixstart, self.unixend)
        self.queryurl = 'http://resin-yeahmobi-214401877.us-east-1.elb.amazonaws.com:18080/report/report?'
        self.small_clicks = 0
        self.small_convs = 0
        self.big_clicks = 0
        self.big_convs = 0
        self.logger = Log('big_small')._get_logger()

    def get_data(self):
        # query from small table
        postsmall = urllib.urlencode({'report_param': self.small})
        rsp = urllib2.build_opener().open(urllib2.Request(self.queryurl, postsmall)).read()
        small_table = json.loads(rsp)['data']['data']
        for data in small_table[1:]:
            self.small_clicks += data[1]
            self.small_convs += data[2]

        # query from big table
        postbig = urllib.urlencode({'report_param': self.big})
        rsp = urllib2.build_opener().open(urllib2.Request(self.queryurl, postbig)).read()
        big_table = json.loads(rsp)['data']['data']
        for data in big_table[1:]:
            self.big_clicks += data[2]
            self.big_convs += data[3]

        click_offset = self.big_clicks - self.small_clicks
        conv_offset = self.big_convs - self.small_convs
        runinfo = '{"query_interval": %s, "big_table": %s, "small_table": %s, "click_offset":%s, "conv_offset": %s}' % (self.endtime,[self.big_clicks, self.big_convs],[self.small_clicks, self.small_convs], click_offset, conv_offset)
        self.logger.info(runinfo)

if __name__ == '__main__':
    bm = big_small()
    bm.get_data()





