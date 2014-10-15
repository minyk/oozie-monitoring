oozie-monitoring
================

Oozie monitoring script for Ganglia

## Install

Install gmond python modules if needed

copy oozie.pyconf to /etc/ganglia/conf.d/
copy oozie_ganglia.py to /usr/lib64/ganglia/python_modules

restart gmond

## Metrics

* The number of workflows
 - failed
 - suspended
 - succeeded
 - prep
 - running
 - total

* JVM Metrics
 - max
 - used

## Acknowledgements

This project inspired by: 
- http://github.com/andreisavu/zookeeper-monitoring
- https://github.com/kristopherkane/oozie_workflow_status_check

Thank you for great projects!
