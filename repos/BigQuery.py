from mapreduce import base_handler
from oauth2client.appengine import AppAssertionCredentials
import mapreduce.third_party.pipeline as pipeline
import mapreduce.third_party.pipeline.common as pipeline_common
import logging

logger = logging.getLogger('pipeline')

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery'
)

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery', 'v2', http=http)

# https://developers.google.com/bigquery/docs/reference/v2/jobs/insert
# can only have one child

class BqCheck(base_handler.PipelineBase):
    def run(self, projectId, jobId, delays=10):
        jobs = service.jobs()
        status = jobs.get(
            projectId=projectId,
            jobId=jobId
        ).execute()

        if status['status']['state'] == 'PENDING' or status['status']['state'] == 'RUNNING':
            delay = yield pipeline_common.Delay(seconds=delays)
            with pipeline.After(delay):
                yield BqCheck(projectId, jobId)
        else:
            if status['status']['state'] == "DONE":
                if 'errorResult' in status['status']:
                    logger.error("bq failed %s " % status)
                else:
                    logger.info("bq success %s" % status)

                yield pipeline_common.Return(status)


class BqApi(base_handler.PipelineBase):
    def run(self, projectId, resourceType, method, body, *args, **kwargs):
        resource = getattr(service, resourceType)()
        method = getattr(resource, method)

        kwargs['projectId'] = projectId
        kwargs['body'] = body

        result = method(
            *args,
            **kwargs
        ).execute()

        checked = yield BqCheck(projectId, result['jobReference']['jobId'])
        with pipeline.After(checked):
            yield pipeline_common.Return(result['jobReference']['jobId'])


class BqQuery2Func(base_handler.PipelineBase):
    def run(self, projectId, query, funcPath, funcParams, timeoutMs=0):
        jobId = yield BqApi(projectId, 'jobs', 'query', {
            "query": query,
            "timeoutMs": timeoutMs
        })

        with pipeline.After(jobId):
            yield BqResults2Func(projectId, jobId, funcPath, funcParams, timeoutMs)


class BqResults2Func(base_handler.PipelineBase):
    def run(self, projectId, jobId, funcPath, funcParams, timeoutMs=0):
        jobs = service.jobs()
        queryReply = jobs.getQueryResults(
            projectId=projectId,
            jobId=jobId,
            timeoutMs=timeoutMs
        ).execute()

        rows = []
        if('rows' in queryReply):
            currentRow = len(queryReply['rows'])

            while('rows' in queryReply and currentRow < queryReply['totalRows']):
                queryReply = jobCollection.getQueryResults(
                    projectId=projectId,
                    jobId=jobId,
                    startIndex=currentRow,
                    timeoutMs=timeoutMs
                ).execute()

                if('rows' in queryReply):
                    currentRow += len(queryReply['rows'])
                    rows.extend(queryReply['rows'])

        func = load_module(funcPath)
        args = funcParams.get('args', [])
        kwargs = funcParams.get('kwargs', {})
        r = func(rows, *args, **kwargs)
        yield pipeline_common.Return(r)

