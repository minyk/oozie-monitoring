oozie-monitoring
================

Oozie monitoring script for Ganglia

## Install

Install gmond python modules if needed

copy oozie.pyconf to /etc/ganglia/conf.d/
copy oozie_ganglia.py to /usr/lib64/ganglia/python_modules

restart gmond

## Metrics

Currently, only number of workflows are provided below:
- failed
- suspended
- succeeded
- prep
- running
- total
