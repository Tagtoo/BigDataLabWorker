
import unittest
import webtest
import webapp2
from google.appengine.ext import testbed
import os
from __mimic import datastore_tree as dt

def copy_tree(folder, tree, dest):
    for ipath in os.listdir(folder):
        if os.path.isfile(folder + '/' + ipath):
            tree.SetFile(dest + '/' + ipath, open(folder+'/'+ipath).read())

        else:
            copy_tree(folder + '/' + ipath, tree, dest + '/' + ipath)


class EnvTest(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_all_stubs()
        self.cookies = {}

        self.tree = dt.DatastoreTree()

    def test_mimic(self):
        copy_tree('./tests/env', self.tree, 'fake')

        for i in os.listdir('./tests/env'):
            self.assertEqual(self.tree.GetFileContents('fake/%s'%i), open('./tests/env/%s'%i).read())


