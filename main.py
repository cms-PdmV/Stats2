from flask import Flask, render_template, request, make_response, redirect
from flask_restful import Api
from couchdb_database import Database
from utils import setup_file_logging
from stats_update import StatsUpdate
import json
import time
import argparse


app = Flask(__name__,
            static_folder="./html/static",
            template_folder="./html")
api = Api(app)


@app.route('/get_json/<string:workflow_name>')
@app.route('/api/get_json/<string:workflow_name>')
def html_view_json(workflow_name):
    database = Database()
    workflow = database.get_workflow(workflow_name)
    if workflow is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(workflow, indent=2, sort_keys=True), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


# HTML responses
@app.route('/')
@app.route('/<int:page>')
def html_get(page=0):
    database = Database()
    workflows = get_page(page)
    pages = [page, page > 0, database.PAGE_SIZE == len(workflows)]
    workflows = list(filter(lambda req: '_design' not in req['_id'], workflows))
    datetime_format = '%Y&#8209;%m&#8209;%d&nbsp;%H:%M:%S'
    for req in workflows:
        if '_design' in req['_id']:
            continue

        req['FirstStatus'] = ''
        req['LastStatus'] = ''
        if len(req.get('RequestTransition', [])) > 0:
            first_transition = req['RequestTransition'][0]
            last_transition = req['RequestTransition'][-1]
            if 'Status' in first_transition and 'UpdateTime' in first_transition:
                req['FirstStatus'] = '%s (%s)' % (first_transition['Status'],
                                                  time.strftime(datetime_format, time.localtime(first_transition['UpdateTime'])))

            if 'Status' in last_transition and 'UpdateTime' in last_transition:
                req['LastStatus'] = '%s (%s)' % (last_transition['Status'],
                                                 time.strftime(datetime_format, time.localtime(last_transition['UpdateTime'])))

        req['LastUpdate'] = time.strftime(datetime_format, time.localtime(req['LastUpdate']))
        req['Requests'] = get_unique_list(req.get('Requests', []))
        req['Campaigns'] = get_unique_list(req.get('Campaigns', []))
        calculated_datasets = []
        total_events = req.get('TotalEvents', 0)
        for dataset in req['OutputDatasets']:
            new_dataset = {'Name': dataset,
                           'Events': 0,
                           'Type': 'NONE',
                           'CompletedPerc': '0.0',
                           'Datatier': dataset.split('/')[-1],
                           'Size': -1,
                           'NiceSize': '0B'}
            for history_entry in reversed(req['EventNumberHistory']):
                history_entry = history_entry['Datasets']
                if dataset in history_entry:
                    new_dataset['Events'] = history_entry[dataset]['Events']
                    new_dataset['Type'] = history_entry[dataset]['Type']
                    new_dataset['Size'] = history_entry[dataset].get('Size', -1)
                    new_dataset['NiceSize'] = get_nice_size(new_dataset['Size'])
                    if total_events > 0:
                        new_dataset['CompletedPerc'] = '%.2f' % (new_dataset['Events'] / total_events * 100.0)

                    break

            calculated_datasets.append(new_dataset)

        req['OutputDatasets'] = calculated_datasets

    last_stats_update = database.get_setting('last_dbs_update_date', 0)
    last_stats_update = time.strftime(datetime_format, time.localtime(last_stats_update))

    return render_template('index.html',
                           last_stats_update=last_stats_update,
                           workflows=workflows,
                           total_workflows=database.get_workflow_count(),
                           pages=pages,
                           query=request.query_string.decode('utf-8'))


@app.route('/update/<string:workflow_name>')
def html_update(workflow_name):
    StatsUpdate().perform_update(workflow_name=workflow_name)
    return redirect("/?workflow_name=" + workflow_name, code=302)


@app.route('/search')
def html_search():
    database = Database()
    q = request.args.get('q')
    if not q:
        return redirect("/stats", code=302)

    q = q.strip()
    if database.get_workflows_with_prepid(q):
        return redirect("/stats?prepid=" + q, code=302)
    elif database.get_workflows_with_dataset(q):
        return redirect("/stats?dataset=" + q, code=302)
    elif database.get_workflows_with_campaign(q):
        return redirect("/stats?campaign=" + q, code=302)
    elif database.get_workflows_with_type(q):
        return redirect("/stats?type=" + q, code=302)
    elif database.get_workflows_with_processing_string(q):
        return redirect("/stats?processing_string=" + q, code=302)
    elif database.get_workflows_with_request(q):
        return redirect("/stats?request=" + q, code=302)

    return redirect("/stats?workflow_name=" + q, code=302)


# JSON responses
@app.route('/api/get')
@app.route('/api/get/<int:page>')
def api_get(page=0):
    workflows = get_page(page)
    workflows = list(filter(lambda req: '_design' not in req['_id'], workflows))
    response = make_response(json.dumps(workflows,
                                        indent=4,
                                        sort_keys=True),
                             200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/api/update/<string:workflow_name>')
def api_update(workflow_name):
    StatsUpdate().perform_update(workflow_name=workflow_name)
    return redirect("/api/get?workflow_name=" + workflow_name, code=302)


# Actual get method
def get_page(page=0):
    database = Database()
    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')
    campaign = request.args.get('campaign')
    workflow_type = request.args.get('type')
    workflow_name = request.args.get('workflow_name')
    processing_string = request.args.get('processing_string')
    request_name = request.args.get('request')
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
        elif processing_string is not None:
            workflows = database.get_workflows_with_processing_string(processing_string, page=page, include_docs=True)
        elif request_name is not None:
            workflows = database.get_workflows_with_request(request_name, page=page, include_docs=True)
        else:
            workflows = database.get_workflows(page=page, include_docs=True)

    if prepid is not None or dataset is not None or request_name is not None:
        workflows = sorted(workflows, key=lambda wf: '_'.join(wf.get('RequestName').split('_')[-3:-1]))

    return workflows


def get_unique_list(input_list):
    new_list = []
    for element in input_list:
        if element not in new_list:
            new_list.append(element)

    return new_list


def get_nice_size(size):
    base = 1000.0
    if size < base:
        return '%sB' % (size)
    elif size < base**2:
        return '%.2fKB' % (size / base)
    elif size < base**3:
        return '%.2fMB' % (size / base**2)
    elif size < base**4:
        return '%.2fGB' % (size / base**3)
    else:
        return '%.2fTB' % (size / base**4)


def run_flask():
    setup_file_logging()
    parser = argparse.ArgumentParser(description='Stats2')
    parser.add_argument('--port',
                        help='Port, default is 8001')
    parser.add_argument('--host',
                        help='Host IP, default is 127.0.0.1')
    parser.add_argument('--debug',
                        help='Run Flask in debug mode',
                        action='store_true')
    args = vars(parser.parse_args())
    port = args.get('port', None)
    host = args.get('host', None)
    debug = args.get('debug', False)
    if not port:
        port = 8001

    if not host:
        host = '127.0.0.1'

    app.run(host=host,
            port=port,
            debug=debug,
            threaded=True)


if __name__ == '__main__':
    run_flask()
