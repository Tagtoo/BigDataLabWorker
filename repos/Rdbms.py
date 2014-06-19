from mapreduce import base_handler, mapreduce_pipeline
import mapreduce.third_party.pipeline.common as pipeline_common
import rdbms

class Query(base_handler.PipelineBase):
    def run(self, instance, database, query):
        with rdbms.Connection(instance, database) as connect:
            cursor = connect.cursor()
            cursor.execute(query)
            connect.commit()
            yield pipeline_common.Return(cursor.fetchall())

