import sys
import os

def include_sys(paths):
    for p in paths:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), p))

include_sys([
    'lib',
    'lib/httplib2/python2',
    'lib/google-api-python-client',
    'lib/google-api-python-client/oauth2client',
    'lib/handlers',
    'lib/TaskWorker'
])


# https://developers.google.com/appengine/docs/python/tools/appengineconfig#Python_Module_Configuration
# based on the article, the following code should have the same affect

# config mapreduce config
# it will affet the instance restart times, and the progress update rate
# import mapreduce.parameters
# mapreduce.parameters.config._SLICE_DURATION_SEC = 5*60
mapreduce__SLICE_DURATION_SEC = 5 * 60
