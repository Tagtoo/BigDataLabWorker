from mapreduce import base_handler, mapreduce_pipeline
import mapreduce.third_party.pipeline.common as pipeline_common
import MySQLdb
# use the recommand way to connect database
# https://developers.google.com/appengine/docs/python/cloud-sql/?hl=zh-TW

class Query(base_handler.PipelineBase):
    def run(self, instance, database, query):
        db = MySQLdb.connect(unix_socket="/cloudsql/" + instance, db=database, user="root")
        try:
            cursor = db.cursor()
            cursor.execute(query)
            db.commit()
            yield pipeline_common.Return(cursor.fetchall())
        except MySQLdb.Error, e:
            # TODO: db.rollback will raise exception while db is MyISAM
            db.rollback()
            raise
        finally:
            db.close()
