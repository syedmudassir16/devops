from __future__ import absolute_import
import os
from celery import Task
from celery import Celery
from celery.app.registry import TaskRegistry
from Ign_utils.general_utils import read_json, read_yaml
registry = TaskRegistry()
PATH = os.path.dirname(os.path.abspath(__file__))

config_data     = read_yaml(os.path.join(PATH, "config.yaml"))
project_config  = config_data["projectname"]
global_config   = config_data["global_config"]
#redis_url       = global_config["redis_url"]

class DummyClass(Task):
    def __init__(self, projectname, subprojectname):
        self.projectname     = projectname

        self.sub_classlist   = subprojectname.split('$c$')
        self.num_classes     = len(self.sub_classlist) + 1

        self.sub_projectname = subprojectname.replace('$c$', '_')
        self.sub_projectname = '_'.join(self.sub_classlist)
        self.name = self.sub_projectname
    
    def run(self):
        return 0


for projectname, info_dict in project_config.items():
    node_dict = info_dict['node_list']
    for k, node_info in node_dict.items():
        if node_info['active'] is True:
            sub_project_name = node_info['node']
            registry.register(DummyClass(projectname, sub_project_name))

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL') if os.environ.get('CELERY_BROKER_URL') else global_config["redis_url"]
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND') if os.environ.get('CELERY_RESULT_BACKEND') else global_config["redis_url"]

app = Celery('DNN_Module',
             broker=CELERY_BROKER_URL, # ML1 - redis://192.168.38.7:6379/0
             backend=CELERY_RESULT_BACKEND,
             tasks=registry)

if __name__ == '__main__':
    app.start()
