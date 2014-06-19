from mapreduce import base_handler, mapreduce_pipeline

class Mapper(base_handler.PipelineBase):
    def run(self, *args, **kwargs):
        yield mapreduce_pipeline.MapperPipeline(*args, **kwargs)


class MapReduce(base_handler.PipelineBase):
    def run(self, *args, **kwargs):
        yield mapreduce_pipeline.MapReducePipeline(*args, **kwargs)

