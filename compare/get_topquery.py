#! /usr/bin/env python
#--*-- coding=utf-8 --*--


"""
    get top n query from online query list
"""

from __future__ import division
import json



# global variable
ONLINE_FILE = '../extern/querystatic.list'
DRUID_QUERY = '../extern/druidquery.list'
MYSQL_QUERY = '../extern/mysqlquery.list'


class query(object):
    def __init__(self):
        pass
    def get_topn(self, target_percent): # return lines which frequency lagger than target_percent
        try:
            fobj = open(ONLINE_FILE, 'r')
            online_query = fobj.readlines()
        except IOError, e:
            raise e
        else:
            fobj.close()
        filter_same = list(set(online_query))
        convert_to_json = [json.loads(query) for query in filter_same]
        sort_json_query = sorted(convert_to_json, key=lambda query:query["queryCounts"], reverse=True)

        # get total query counts
        totalcount = 0
        for query in sort_json_query:
            totalcount += query.get('queryCounts', 0)

        # get every signle line percent
        current_count = 0
        for index, query in enumerate(sort_json_query):
            current_count += query['queryCounts']
            current_percent = current_count / totalcount
            if current_percent >= target_percent:
                break


        topn_list = []
        for query in sort_json_query[:index+1]:
            topn_list.append(query.get('dimensions', None))

        print topn_list

        #write to druid query
        fobj = open(DRUID_QUERY, 'w')
        for query in topn_list:
            fobj.writelines(str(query) + '\n')
        fobj.close()


        fobj = open(MYSQL_QUERY,'w')
        for query in topn_list:
            pass
        fobj.close()







if __name__ == '__main__':
    queryer = query()
    print queryer.get_topn(0.97)




