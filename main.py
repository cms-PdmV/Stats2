from flask import Flask, render_template, request, make_response, redirect
from flask_restful import Api
from couchdb_database import Database
from utils import setup_file_logging, make_simple_request
from stats_update import StatsUpdate
import json
import time


app = Flask(__name__,
            static_folder="./html/static",
            template_folder="./html")
api = Api(app)


def check_with_old_stats(workflows):
    """
    Delete this if Stats2 is used as prod service
    """
    for req in workflows:
        stats_url = "http://vocms074:5984/stats/%s" % req['_id']
        try:
            stats_req = make_simple_request(stats_url)
            req['OldCompletion'] = '%.2f' % (float(stats_req['pdmv_completion_in_DAS']))
            if stats_req['pdmv_expected_events'] == req['TotalEvents']:
                req['TotalEventsEqual'] = 'equal'
            else:
                req['TotalEventsEqual'] = 'not_equal'
                req['TotalEventsStats'] = stats_req['pdmv_expected_events']
        except:
            req['TotalEventsEqual'] = 'not_found'


@app.route('/')
@app.route('/<int:page>')
def index(page=0):
    database = Database()
    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')
    campaign = request.args.get('campaign')
    workflow_type = request.args.get('type')
    workflow_name = request.args.get('workflow_name')
    check = request.args.get('check')
    if page < 0:
        page = 0

    if workflow_name is not None:
        req = database.get_workflow(workflow_name)
        if req is not None:
            workflows = [req]
        else:
            workflows = []

    else:
        if prepid is not None:
            workflows = database.get_workflows_with_prepid(prepid, page=page, include_docs=True)
        elif dataset is not None:
            workflows = database.get_workflows_with_dataset(dataset, page=page, include_docs=True)
        elif campaign is not None:
            workflows = database.get_workflows_with_campaign(campaign, page=page, include_docs=True)
        elif workflow_type is not None:
            workflows = database.get_workflows_with_type(workflow_type, page=page, include_docs=True)
        else:
            workflows = database.get_workflows(page=page, include_docs=True)

    if check is not None:
        check_with_old_stats(workflows)

    pages = [page, page > 0, database.PAGE_SIZE == len(workflows)]
    workflows = list(filter(lambda req: '_design' not in req['_id'], workflows))
    for req in workflows:
        req['DonePercent'] = '0.00'
        req['OpenPercent'] = '0.00'
        req['LastDatasetType'] = 'NONE'
        req['LastDataset'] = ''
        req['DoneEvents'] = '0'
        req['LastUpdate'] = time.strftime('%Y&#8209;%m&#8209;%d&nbsp;%H:%M:%S', time.localtime(req['LastUpdate']))

        if len(req['OutputDatasets']) == 0:
            continue

        if len(req['EventNumberHistory']) == 0:
            continue

        last_dataset = req['OutputDatasets'][-1:][0]
        last_history = req['EventNumberHistory'][-1:][0]
        if last_dataset not in last_history['Datasets']:
            continue

        calculated_dataset = last_history['Datasets'][last_dataset]
        dataset_type = calculated_dataset['Type']
        req['LastDatasetType'] = dataset_type
        req['LastDataset'] = last_dataset
        done_events = calculated_dataset['Events']
        req['DoneEvents'] = done_events
        if 'TotalEvents' not in req:
            continue

        if req['TotalEvents'] > 0:
            total_events = req['TotalEvents']
            req['DonePercent'] = '%.2f' % (done_events / total_events * 100.0)

    return render_template('index.html',
                           workflows=workflows,
                           total_workflows=database.get_workflow_count(),
                           pages=pages,
                           query=request.query_string.decode('utf-8'))


@app.route('/update/<string:workflow_name>')	
def update(workflow_name):	
    StatsUpdate().perform_update(workflow_name=workflow_name)	
    return redirect("/0?workflow_name=" + workflow_name, code=302)


@app.route('/get/<string:workflow_name>')
def get_one(workflow_name):
    database = Database()
    workflow = database.get_workflow(workflow_name)
    if workflow is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(workflow), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/view_json/<string:workflow_name>')
def get_nice_json(workflow_name):
    database = Database()
    workflow = database.get_workflow(workflow_name)
    if workflow is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(workflow, indent=4, sort_keys=True), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


def run_flask():
    setup_file_logging()
    app.run(host='0.0.0.0',
            port=443,
            debug=True,
            threaded=True,
            ssl_context=('/home/jrumsevi/localhost.crt', '/home/jrumsevi/localhost.key'))


if __name__ == '__main__':
    run_flask()
