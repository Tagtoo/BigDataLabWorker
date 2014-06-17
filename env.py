from __mimic import datastore_tree, target_env

class TaskVirtualEnvironment(target_env.TargetEnvironment):
    # Based on TargetEnvironment
    def __init__(self, tree, namespace=""):
        self._tree = tree
        self._namespace = namespace
        self._active = False
        self._saved_sys_path = None
        self._saved_sys_modules = None
        self._test_portal = None
        self._saved_open = open
        self._main_method = ''
        self._wsgi_app_name = None
        self._patches = []
        self._static_file_patterns = None
        self._skip_files_pattern = None
