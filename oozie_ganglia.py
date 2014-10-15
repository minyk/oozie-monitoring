#!/usr/bin/python

""" Python Ganglia Module for Oozie monitoring

Inspired by: 
 - http://github.com/andreisavu/zookeeper-monitoring
 - https://github.com/kristopherkane/oozie_workflow_status_check

Copy this file to /usr/lib64/ganglia/python_modules

Metric Group oozie
Metric
 - oozie.jobs.total
 - oozie.jobs.failed
 - oozie.jobs.suspended
 - oozie.jobs.killed
 - oozie.jobs.succeeded
 - oozie.jobs.prepare
 - oozie.jobs.running
 - oozie.jvm.mem.total
 - oozie.jvm.mem.free
 - oozie.jvm.mem.max

"""

import sys
import socket
import time
import re
import copy
import urllib
import json
import time
import datetime
from time import gmtime, strftime
import urllib2

from StringIO import StringIO

TIME_BETWEEN_QUERIES = 20
OOZIE_METRICS = {
    'time' : 0,
    'data' : {}
}
OOZIE_LAST_METRICS = copy.deepcopy(OOZIE_METRICS)

class OozieServer(object):

    def __init__(self, host='localhost', port='11000', timeout=1):
        self._host = host
        self._port = port
        self._timeout = timeout

    def get_stats(self):
        """ Get Oozie server stats as a map """
        global OOZIE_METRICS, OOZIE_LAST_METRICS
        # update cache
        OOZIE_METRICS = {
            'time' : time.time(),
            'data' : {}
        }
        
        jobs_data = self._get_jobs()
        jvm_data = self._get_jvm()
        data = dict(jobs_data, **jvm_data)
        
        OOZIE_METRICS['data'] = data
        OOZIE_LAST_METRICS = copy.deepcopy(OOZIE_METRICS)
        return data

    def _get_jvm(self):
        result = {}
        uri = "http://" + self._host + ":" + self._port + "/oozie/v1/admin/instrumentation"
        mem_free = 0
        mem_used = 0
        mem_max = 0

        try:
            raw_json = urllib.urlopen(uri)
        except:
            print "Error connecting to the Oozie server"
            return result

        #Create a JSON object
        try:
            json_object = json.load(raw_json)
        except:
            print "Error parsing the JSON from Oozie"
            return result

        jvm = []

        for variable in json_object[u'variables']:
            if variable[u'group'] == "jvm":
                jvm = variable[u'data']

        for data in jvm:
            if data[u'name'] == "free.memory":
               mem_free = long(data[u'value'])
            if data[u'name'] == "total.memory":
               mem_total = long(data[u'value'])
            if data[u'name'] == "max.memory":
               mem_max = long(data[u'value'])

        result['oozie.jvm.mem.free'] = mem_free
        result['oozie.jvm.mem.total'] = mem_total
        result['oozie.jvm.mem.max'] = mem_max

        return result


    def _get_jobs(self):
        result = {}
        tz = strftime("%Z", gmtime())
        uri = "http://" + self._host + ":" + self._port + "/oozie/v1/jobs?jobType=wf&timezone=%s" % (tz)

        failed_count = 0
        suspended_count = 0
        killed_count = 0
        succeeded_count = 0
        prep_count = 0
        running_count = 0
        workflows = []

        try:
            raw_json = urllib.urlopen(uri)
        except:
            print "Error connecting to the Oozie server"
            return result

        #Create a JSON object
        try:
            json_object = json.load(raw_json)
        except:
            print "Error parsing the JSON from Oozie"
            return result

        #iterate through the json and pull out the workflows
        for job in json_object[u'workflows']:
            row = [job[u'id'], job[u'appName'], job[u'status'], job[u'endTime']]
            workflows.append(row)

        #iterate through the workflows and get status
        for workflow in workflows:
            if workflow[2] == "FAILED":
                failed_count += 1

            elif workflow[2] == "SUSPENDED":
                suspended_count += 1

            elif workflow[2] == "KILLED":
                killed_count += 1

            elif workflow[2] == "SUCCEEDED":
                succeeded_count += 1

            elif workflow[2] == "PREP":
                prep_count += 1

            elif workflow[2] == "RUNNING":
                running_count += 1

        total = failed_count + suspended_count + killed_count + succeeded_count + prep_count + running_count

        result['oozie.jobs.total'] = total
        result['oozie.jobs.failed'] = failed_count
        result['oozie.jobs.suspended'] = suspended_count
        result['oozie.jobs.killed'] = killed_count
        result['oozie.jobs.succeeded'] = succeeded_count
        result['oozie.jobs.prepare'] = prep_count
        result['oozie.jobs.running'] = running_count

        return result

def metric_handler(name):
    if time.time() - OOZIE_LAST_METRICS['time'] > TIME_BETWEEN_QUERIES:
        oozie = OozieServer(metric_handler.host, metric_handler.port, 5)
        try:
            metric_handler.info = oozie.get_stats()
        except Exception, e:
            print >>sys.stderr, e
            metric_handler.info = {}

    return metric_handler.info.get(name, 0)

def metric_init(params=None):
    params = params or {}

    metric_handler.host = params.get('host', 'localhost')
    metric_handler.port = params.get('port', 11000)
    metric_handler.timestamp = 0

    metrics = {
        'oozie.jobs.total' : {'units': 'Job'},
        'oozie.jobs.failed' : {'units': 'Job'},
        'oozie.jobs.suspended' : {'units': 'Job'},
        'oozie.jobs.killed' : {'units': 'Job'},
        'oozie.jobs.succeeded' : {'units': 'Job'},
        'oozie.jobs.prepare' : {'units': 'Job'},
        'oozie.jobs.running' : {'units': 'Job'},
        'oozie.jvm.mem.total' : {'units': 'Bytes'},
        'oozie.jvm.mem.free' : {'units': 'Bytes'}
    }
    metric_handler.descriptors = {}
    for name, updates in metrics.iteritems():
        descriptor = {
            'name': name,
            'call_back': metric_handler,
            'time_max': 90,
            'value_type': 'int',
            'units': '',
            'slope': 'both',
            'format': '%d',
            'groups': 'oozie',
            }
        descriptor.update(updates)
        metric_handler.descriptors[name] = descriptor

    return metric_handler.descriptors.values()

def metric_cleanup():
    pass


if __name__ == '__main__':
    ds = metric_init({'host':'localhost', 'port': '11000'})
    while True:
        for d in ds:
            print "%s=%s" % (d['name'], metric_handler(d['name']))
        time.sleep(10)
