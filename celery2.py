import os
import time
from flask import Flask, request, render_template, jsonify, url_for
from celery import Celery

app = Flask(__name__)

print(app.name)

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# this task will run as a Celery worker process
@celery.task(bind=True)
def long_task(self):
    for i in range(30):
        message = "Doing something long..."
        self.update_state(state='PROGRESS', meta = {'current':i, 'total':30, 'status':message})
        time.sleep(2)
    return {'current': 100, 'total':100, 'status':'Task completed!', 'result': 42}


@app.route('/', methods=['GET','POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')
    return redirect(url_for('index'))


@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus', task_id=task.id)}
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
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info) # this is the exception raised
        }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
    print ("Celery yoo done!")
