from mapreduce import base_handler, mapreduce_pipeline
from oauth2client.appengine import AppAssertionCredentials
import httplib2
from google.appengine.api import memcache
from googleapiclient.discovery import build
import pipeline
import logging

logger = logging.getLogger('pipeline')

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery'
)

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery', 'v2', http=http)

def log2gs(l):
    yield l.resource + '\n'

class Log2Gs(base_handler.PipelineBase):
    """A pipeline to ingest log as CSV in Google Storage
    """

    def run(self, name, mapper, start_time, end_time, version_ids, gsbucketname, shards=255):
        yield mapreduce_pipeline.MapperPipeline(
            name,
            mapper,
            "mapreduce.input_readers.LogInputReader",
            output_writer_spec="mapreduce.output_writers.FileOutputWriter",
            params={
                "input_reader": {
                    "start_time": start_time,
                    "end_time": end_time,
                    "version_ids": version_ids,
                },
                "output_writer": {
                    "filesystem": "gs",
                    "gs_bucket_name": gsbucketname,
                    # "output_sharding": FileOutputWriter.OUTPUT_SHARDING_INPUT_SHARDS,
                },
                "root_pipeline_id": self.root_pipeline_id,
            },
            shards=shards
        )


class BqCheck(base_handler.PipelineBase):
    def run(self, bqproject, job):
        jobs = service.jobs()
        status = jobs.get(
            projectId=bqproject,
            jobId=job
        ).execute()

        if status['status']['state'] == 'PENDING' or status['status']['state'] == 'RUNNING':
            delay = yield pipeline.common.Delay(seconds=1)
            with pipeline.After(delay):
                yield BqCheck(bqproject, job)
        else:
            if status['status']['state'] == "DONE":
                if 'errorResult' in status['status']:
                    logger.error("bq failed %s " % status)
                else:
                    logger.info("bq success %s" % status)

                yield pipeline.common.Return(status)


class Gs2Bq(base_handler.PipelineBase):
    """A pipeline to ingest log csv from Google Storage to Google BigQuery.
    """

    def run(self, files, bqproject, bqdataset, table, fields, overwrite=True):
        jobs = service.jobs()
        gspaths = [f.replace('/gs/', 'gs://') for f in files]
        result = jobs.insert(
            projectId=bqproject,
            body={
                'projectId': bqproject,
                'configuration': {
                    'load': {
                        'sourceUris': gspaths,
                        'schema': {
                            'fields': fields
                        },
                        'destinationTable': {
                            'projectId': bqproject,
                            'datasetId': bqdataset,
                            'tableId': table
                        },
                        'createDisposition': 'CREATE_IF_NEEDED',
                        'writeDisposition': 'WRITE_TRUNCATE' if overwrite else 'WRITE_APPEND',
                        'encoding': 'UTF-8',
                        'sourceFormat': 'NEWLINE_DELIMITED_JSON'
                    }
                }
            }
        ).execute()

        yield BqCheck(bqproject, result['jobReference']['jobId'])


class Log2Bq(base_handler.PipelineBase):
    """A pipeline to ingest log as CSV in Google Big Query
    """

    def run(self, name, mapper, start_time, end_time, version_ids, gsbucketname, bqproject, bqdataset, table, fields, overwrite):
        files = yield Log2Gs(name, mapper, start_time, end_time, version_ids, gsbucketname)
        yield Gs2Bq(files, bqproject, bqdataset, table, fields, overwrite=overwrite)


