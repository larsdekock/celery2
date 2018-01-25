import os
import time
import logging
import datetime
from flask import Flask, request, render_template, jsonify, url_for
from billiard.exceptions import Terminated
from celery import Celery
from celery.signals import task_revoked
from pymongo import MongoClient

logging.getLogger().setLevel(logging.DEBUG)

logging.info("Launching Flask and Redis...")
app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
logging.info("Done.")

print(app.name) # filename in front of .py
print("Started app at " + str(datetime.datetime.utcnow()))


logging.info("Launching Celery...")
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
logging.info("Done.")


# currently does nothing useful, can use this to do something when a task has been revoked
@task_revoked.connect
def on_task_revoked(*args, **kwargs):
    print('Task revoked! Dumping arguments:')
    print(str(kwargs))


# this task will run as a Celery worker process
@celery.task(bind=True, throws=(Terminated,))
def long_task(self):
    try:
        logging.info("Launching MongoDB...")
        mongo = MongoClient('mongodb://localhost:27017')
        db = mongo.celery2
        logging.info("Done.")

        logging.info("Storing new task in database...")
        db.tasks.insert_one( { "task":"new task", "starttime": datetime.datetime.utcnow() } )
        logging.info("Done.")

        for i in range(30):
            message = "Doing something long..."
            self.update_state(state='PROGRESS', meta = {'current':i, 'total':30, 'status':message})
            time.sleep(2)
        return {'current': 100, 'total':100, 'status':'Task completed!', 'result': 42}
    except Terminated as exc:
        print("EXCEPTION")
        print(exc)
    except Exception as exc:
        print("Unknown exception!")
        print(exc)


@app.route('/', methods=['GET','POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')
    return redirect(url_for('index'))


@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus', task_id=task.id), 'task_id': task.id}
    # status 202 is normally used in REST APIs to indicate that a request is in progress
    # Location header is added with a URL that the client can use to obtain status information, i.e. route taskstatus

@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': ' Pending...'
        }
    elif task.state == 'FAILURE':
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info) # this is the exception raised
        }
    elif task.state == 'REVOKED':
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info)
        }
    else:
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
    return jsonify(response)

@app.route('/killtask/<task_id>')
def killtask(task_id):
    try:
        task = long_task.AsyncResult(task_id)
        # task.revoke(terminate=True)
        print("test")
        celery.control.revoke(task_id, terminate=True, signal='SIGKILL')
        print("returned")
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Stopped.'
        }
        return jsonify(response)
    except:
        print("EXCEPTION")
        return ""

if __name__ == '__main__':
    try:
        app.run(debug=True)
    except Exception as exc:
        print("Caught main process exception")
        print(exc)
