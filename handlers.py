import webapp2
import json
import urllib
from lib.handlers import ApiHandler
from google.appengine.api import urlfetch
from google.appengine.ext import ndb


class __TA_Task(ndb.Model):

    """store task & pipeline mapping
    """
    root_pipeline_id = ndb.StringProperty()

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)

    parmas = ndb.JsonProperty()

    @property
    def job_id(self):
        return self.key.id()


class TriggerHandler(ApiHandler):

    def get(self):
        id = self.request.get("id")
        path = self.request.get("path")
        method = self.request.get("method", "get")
        params = self.request.get("params", "")

        method = method.lower()
        assert path
        assert method in ("get", "post")

        if method == "get":
            r = urlfetch.fetch(url=path + "?" + params)
        else:
            r = urlfetch.fetch(
                url=path,
                method=urlfetch.POST
                payload=params
            )

        assert r.status_code == 200
        data = json.loads(r.content)
        assert "root_pipeline_id" in data
        root_pipeline_id = data["root_pipeline_id"]

        # if not assign id, will use root_pipeline_id instead
        task_id = id or root_pipeline_id

        __TA_Task(
            id=task_id,
            root_pipeline_id=root_pipeline_id,
            params={
                "id": id,
                "method": method,
                "path": path,
                "params": params
            }
        ).put()

        return self.output({
            id: task_id
        })


class StatusHandler(ApiHandler):

    def get(self):
        id = self.request.get("id")
        pipeline_id = self.request.get("root_pipeline_id")
        assert id or pipeline_id

        if id:
            task = __TA_Task.get_by_id(id)
            assert task and task.root_pipeline_id
            root_pipeline_id = task.root_pipeline_id
        else:
            root_pipeline_id = pipeline_id

        r = urlfetch.fetch(
            url='/_ah/pipeline/rpc/tree?root_pipeline_id={}'.format(root_pipeline_id)
        )
        assert r.status_code == 200

        r = json.loads(r.content)

        self.output(r)


class StopHandler(ApiHandler):

    def get(self):
        id = self.request.get("id")
        pipeline_id = self.request.get("root_pipeline_id")
        assert id or pipeline_id

        if id:
            task = __TA_Task.get_by_id(id)
            assert task and task.root_pipeline_id
            root_pipeline_id = task.root_pipeline_id
        else:
            root_pipeline_id = pipeline_id

        pipeline = Pipeline.from_id(root_pipeline_id)
        pipeline_key = str(pipeline.key())

        r = urlfetch.fetch(
            url="/_ah/pipeline/abort",
            method=urlfetch.POST,
            payload=urllib.urlencode({
                "pipeline_key": pipeline_key,
                "purpose": "abort"
            }))

        assert r.status_code == 200
        return self.output({
            "id": id
        })

app = webapp2.WSGIApplication([
    (r'.*/trigger', TriggerHandler),
    (r'.*/status', StatusHandler),
    (r'.*/stop', StopHandler),
], debug=True)
