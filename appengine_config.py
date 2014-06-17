import sys
import os

def include_sys(paths):
    for p in paths:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), p))

include_sys([
    'lib',
    'lib/httplib2/python2',
    'lib/google-api-python-client',
    'lib/handlers',
    'lib/TaskWorker'
])
