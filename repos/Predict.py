from mapreduce import base_handler
from oauth2client.appengine import AppAssertionCredentials
from apiclient.http import BatchHttpRequest
import mapreduce.third_party.pipeline as pipeline
import mapreduce.third_party.pipeline.common as pipeline_common
import logging

logger = logging.getLogger('pipeline')

credentials = AppAssertionCredentials(
    scope=[
        'https://www.googleapis.com/auth/prediction',
        'https://www.googleapis.com/auth/devstorage.full_control'
    ]
)

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery', 'v2', http=http)
batch = BatchHttpRequest()

class Check(base_handler.PipelineBase):
    def run(self, projectId, modelId, delays=10):
        result = service.trainedmodels().get(
            project=projectId,
            id=modelId
        ).execute()

        if result['trainingStatus'] == "RUNNING":
            delay = yield pipeline_common.Delay(seconds=delays)
            with pipeline.After(delay):
                yield PredictCheck(projectId, modelId, delays)
        else:
            return result


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


class Train(base_handler.PipelineBase):
    """A helper function, which will send train signal, wait it complete
    and return analyze.
    """
    def run(self, projectId, modelId, storageDataLocation, modelType):
        result = service.trainedmodels().insert(
            project=projectId,
            body={
                "id": modelId,
                "storageDataLocation": storageDataLocation,
                "modelType": modelType
            }
        ).execute()

        checked = yield Check(modelId)
        with pipeline.After(checked):
            result = service.trainedmodels().analyze(
                project=projectId,
                id=modelId
            ).execute()
            return result


class PredictResult(base_handler.PipelineBase):
    def run(self, results, withProbabilities=False):
        values = []
        for result in results:
            if 'outputLabel' in result:
                if withProbabilities:
                    values.append(result['outputMulti'])
                else:
                    values.append(result['outputLabel'])
            else:
                values.append(result['outputValue'])

        return values


class Predict(base_handler.PipelineBase):
    def run(self, projectId, modelId, values, withProbabilities=False):
        # TODO: apply batch

        results = []

        for value in values:
            r = yield Api(projectId, "trainedmodels", body={
                "input": {
                    "csvInstance": value
                }
            }, id=modelId)

            results.append(value)

        yield PredictResult(results, withProbabilities)
