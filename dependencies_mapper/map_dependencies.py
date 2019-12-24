import datetime
from airflow import models
from airflow.operators import DummyOperator
from math import ceil

def gen_dummy(task_name):
    
    error = False
    if " " in task_name:
        error = True

    if "(" in task_name:
        error = True

    if ")" in task_name:
        error = True
    if '"' in task_name:
        error = True

    if error:
        print(task_name)

    return DummyOperator(task_id=task_name) 

def count(needle, haystack):
    # prevent side effects

    original = str(haystack)
    modified = str(haystack).replace(needle, "")

    return modified, ceil((len(original) - len(modified)) / len(needle))

with models.DAG(
        dag_id='DependencyFlowChart',
        schedule_interval=datetime.timedelta(days=1),
        start_date=datetime.datetime(2019, 12, 12)) as dag: 
    
    filename = "/home/daimonie/share/shared-scripts/dependencies.txt"
    print(f"Parsing {filename}")

    list_of_tasks = {}

    with open(filename, 'r') as fh:

        line = fh.readline()
        cnt = 1

        last_parent_at_level = {}
        for i in range(10):
            last_parent_at_level[f"level_{i}"] = ""

        while line:
 
            name, tabs = count(" "*4, line)

            name = name.replace("\n", "")

            if name not in list_of_tasks:
                list_of_tasks[name] = gen_dummy(name)
                print(f"My name is {name}")

            if tabs > 0:

                parent = last_parent_at_level["level_{tabs}".format(tabs = (tabs-1))] 

                print(f"My name is [{name}] and my parent is [{parent}]")

                list_of_tasks[name] << list_of_tasks[parent]

            # Basically we keep the last task at every level
            last_parent_at_level[f"level_{tabs}"] = name
            
            line = fh.readline()
            cnt += 1 

print(f"Finally, {cnt} parts")