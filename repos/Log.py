from mapreduce import base_handler, mapreduce_pipeline
import pipeline
import logging
from .BigQuery import Gs2Bq
from .MapReduce import Mapper

logger = logging.getLogger('pipeline')


class Log2Gs(base_handler.PipelineBase):
    """A pipeline to ingest log as CSV in Google Storage
    """

    def run(self, name, mapper, start_time, end_time, version_ids, gsbucketname, shards=255):
        yield Mapper(
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


class Log2Bq(base_handler.PipelineBase):
    """A pipeline to ingest log as CSV in Google Big Query
    """

    def run(self, name, mapper, start_time, end_time, version_ids, gsbucketname, bqproject, bqdataset, table, fields, overwrite):
        files = yield Log2Gs(name, mapper, start_time, end_time, version_ids, gsbucketname)
        yield Gs2Bq(files, bqproject, bqdataset, table, fields, overwrite=overwrite)


