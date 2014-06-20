from mapreduce import base_handler
from oauth2client.appengine import AppAssertionCredentials
import mapreduce.third_party.pipeline as pipeline
import mapreduce.third_party.pipeline.common as pipeline_common
import logging

# define some common query

logger = logging.getLogger('pipeline')

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery'
)

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery', 'v2', http=http)

# https://developers.google.com/bigquery/docs/reference/v2/jobs/insert
# can only have one child

def load_module(cls_path):
    module_path, class_name = ".".join(cls_path.split('.')[:-1]), cls_path.split('.')[-1]
    mod = __import__(module_path, fromlist=[class_name])
    return getattr(mod, class_name)


class Check(base_handler.PipelineBase):
    def run(self, projectId, jobId, delays=10):
        jobs = service.jobs()
        status = jobs.get(
            projectId=projectId,
            jobId=jobId
        ).execute()

        if status['status']['state'] == 'PENDING' or status['status']['state'] == 'RUNNING':
            delay = yield pipeline_common.Delay(seconds=delays)
            with pipeline.After(delay):
                yield Check(projectId, jobId, delays)
        else:
            if status['status']['state'] == "DONE":
                if 'errorResult' in status['status']:
                    logger.error("bq failed %s " % status)
                else:
                    logger.info("bq success %s" % status)

                return status


class Api(base_handler.PipelineBase):
    def run(self, projectId, resourceType, method, body, *args, **kwargs):
        resource = getattr(service, resourceType)()
        method = getattr(resource, method)

        kwargs['projectId'] = projectId
        kwargs['body'] = body

        result = method(
            *args,
            **kwargs
        ).execute()

        return result


class JobSync(base_handler.PipelineBase):
    def run(self, projectId, method, body, *args, **kwargs):
        jobs = service.jobs()
        method = getattr(resource, method)

        kwargs['projectId'] = projectId
        kwargs['body'] = body

        result = method(
            *args,
            **kwargs
        ).execute()

        checked = yield Check(projectId, result['jobReference']['jobId'])
        with pipeline.After(checked):
            return result['jobReference']['jobId']


class Query(base_handler.PipelineBase):
    def run(self, projectId, query):
        jobId = yield JobSync(projectId, "query", {
            "query": query,
        })
        return QueryResults(projectId, jobId)


class QueryResults(base_handler.PipelineBase):
    def run(self, projectId, jobId, timeoutMs=0):
        jobs = service.jobs()
        queryReply = jobs.getQueryResults(
            projectId=projectId,
            jobId=jobId,
            timeoutMs=0
        ).execute()

        rows = []
        if('rows' in queryReply):
            currentRow = len(queryReply['rows'])

            while('rows' in queryReply and currentRow < queryReply['totalRows']):
                queryReply = jobs.getQueryResults(
                    projectId=projectId,
                    jobId=jobId,
                    startIndex=currentRow,
                    timeoutMs=timeoutMs
                ).execute()

                if('rows' in queryReply):
                    currentRow += len(queryReply['rows'])
                    rows.extend(queryReply['rows'])

        return rows


class Query2Func(base_handler.PipelineBase):
    def run(self, projectId, query, funcPath, funcParams, timeoutMs=0):
        rows = yield Query(projectId, query, timeoutMs)
        yield Results2Func(projectId, rows, funcPath, funcParams)


class Results2Func(base_handler.PipelineBase):
    def run(self, projectId, rows, funcPath, funcParams):
        rows = query_results(projectId, jobId)

        func = load_module(funcPath)
        args = funcParams.get('args', [])
        kwargs = funcParams.get('kwargs', {})
        r = func(rows, *args, **kwargs)

        return r


class Load(base_handler.PipelineBase):
    def run(self, projectId, datasetId, tableId, sourceUris, fields, sourceFormat="CSV", mode="w+", skipLeadingRows=0, **params):
        createDisposition = "CREATE_IF_NEEDED" if '+' in mode else "CREATE_NEVER"

        if 'w' in mode:
            writeDisposition = 'WRITE_TRUNCATE'
        elif 'a' in mode:
            writeDisposition = 'WRITE_APPEND'
        else:
            writeDisposition = "WRITE_EMPTY"

        config = {
            "sourceUris": sourceUris,
            "schema": {
                "fields": fields
            },
            "destinationTable": {
                "projectId": projectId,
                "datasetId": datasetId,
                "tableId": tableId
            },
            'createDisposition': createDisposition,
            'writeDisposition': writeDisposition,
            'sourceFormat': sourceFormat,
            "skipLeadingRows": skipLeadingRows
        }

        config.update(params)

        return JobSync(
            projectId,
            method="insert",
            body={
                "projectId": projectId,
                "configuration": {
                    "load": config
                }
            })


class Extract(base_handler.PipelineBase):
    def run(self, projectId, datasetId, tableId, destinationUris, destinationFormat="CSV", printHeader=True, **params):
        config = {
            "sourceTable": {
                "projectId": projectId,
                "datasetId": datasetId,
                "tableId": tableId
            },
            "destinationUris": destinationUris,
            "destinationFormat": destinationFormat,
            "printHeader": printHeader
        }

        config.update(params)

        return JobSync(
            projectId,
            method="extract",
            body={
                "projectId": projectId,
                "configuration": {
                    "extract": config
                }
            })
