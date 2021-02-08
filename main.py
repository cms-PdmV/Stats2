"""
Module that starts webserver and has all it's endpoints
"""
import json
import time
import argparse
import re
from flask import Flask, render_template, request, make_response, redirect
from flask_restful import Api
from couchdb_database import Database
from utils import setup_console_logging, get_unique_list, get_nice_size


app = Flask(__name__,
            static_folder='./html/static',
            template_folder='./html')
api = Api(app)


@app.route('/get_json/<string:workflow_name>')
@app.route('/api/get_json/<string:workflow_name>')
def html_view_json(workflow_name):
    """
    Return one workflow
    """
    database = Database()
    workflow = database.get_workflow(workflow_name)
    if workflow is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(workflow, indent=2, sort_keys=True), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/api/fetch')
def html_api_fetch():
    """
    Return workflows for a given q= query
    """
    database = Database()
    page = 0
    workflows = []
    fetched = [{}]
    while len(fetched) > 0 and page < 10:
        workflows.extend(get_page(page))
        page += 1

    response = make_response(json.dumps(workflows, indent=2, sort_keys=True), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


def matches_regex(value, regex):
    """
    Check if given string fully matches given regex
    """
    matcher = re.compile(regex)
    match = matcher.fullmatch(value)
    if match:
        return True

    return False


def get_service_type(workflow):
    prepid = workflow.get('PrepID')
    if not prepid:
        return 'unknown'

    if matches_regex(prepid, '^ReReco-.*-[0-9]{5}$'):
        return 'rereco_machine'

    if matches_regex(prepid, '^ReReco-.*$'):
        return 'rereco'

    if matches_regex(prepid, '^CMSSW_.*-[0-9]{5}$'):
        return 'relval_machine'

    if matches_regex(prepid, '^CMSSW_.*'):
        return 'relval'

    if matches_regex(prepid, '^(task_)?[A-Z0-9]{3}-.*-[0-9]{5}$'):
        return 'mc'

    return 'unknown'


# HTML responses
@app.route('/')
@app.route('/<int:page>')
def html_get(page=0):
    """
    Return HTML of selected page
    This method also prettifies some dates, makes campaigns and requests lists unique,
    calculates completness of output datasets
    """
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
        if req.get('RequestTransition', []):
            first_transition = req['RequestTransition'][0]
            last_transition = req['RequestTransition'][-1]
            if 'Status' in first_transition and 'UpdateTime' in first_transition:
                status = first_transition['Status']
                update_time = time.strftime(datetime_format,
                                            time.localtime(first_transition['UpdateTime']))
                req['FirstStatus'] = f'{status} ({update_time})'

            if 'Status' in last_transition and 'UpdateTime' in last_transition:
                status = last_transition['Status']
                update_time = time.strftime(datetime_format,
                                            time.localtime(last_transition['UpdateTime']))
                req['LastStatus'] = f'{status} ({update_time})'

        req['LastUpdate'] = time.strftime(datetime_format, time.localtime(req['LastUpdate']))
        req['Requests'] = get_unique_list(req.get('Requests', []))
        req['Campaigns'] = get_unique_list(req.get('Campaigns', []))
        service_type = get_service_type(req)
        req['ServiceType'] = service_type
        if service_type == 'relval_machine':
            req['Campaigns'] = [{'name': x, 'links': [{'name': 'RelVal', 'link': 'https://cms-pdmv.cern.ch/relval/campaigns?prepid=%s' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Campaigns']]
            if len(req['Requests']) == 0:
                req['Requests'] = [req['PrepID']]

            req['Requests'] = [{'name': x, 'links': [{'name': 'RelVal', 'link': 'https://cms-pdmv.cern.ch/relval/relvals?prepid=%s' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Requests']]

        elif service_type == 'rereco_machine':
            req['Campaigns'] = [{'name': x, 'links': [{'name': 'ReReco', 'link': 'https://cms-pdmv.cern.ch/rereco/subcampaigns?prepid=%s-*' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Campaigns']]
            if len(req['Requests']) == 0:
                req['Requests'] = [req['PrepID']]

            req['Requests'] = [{'name': x, 'links': [{'name': 'ReReco', 'link': 'https://cms-pdmv.cern.ch/rereco/requests?prepid=%s' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Requests']]

        elif service_type == 'mc':
            req['Campaigns'] = [{'name': x, 'links': [{'name': 'McM', 'link': 'https://cms-pdmv.cern.ch/mcm/campaigns?prepid=%s' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Campaigns']]
            req['Requests'] = [{'name': x, 'links': [{'name': 'McM', 'link': 'https://cms-pdmv.cern.ch/mcm/requests?prepid=%s' % (x)}, {'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Requests']]

        else:
            req['Campaigns'] = [{'name': x, 'links': [{'name': 'pMp', 'link': 'https://cms-pdmv.cern.ch/pmp/historical?r=%s' % (x)}]} for x in req['Campaigns']]
            req['Requests'] = []

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
                        percentage = new_dataset['Events'] / total_events * 100.0
                        new_dataset['CompletedPerc'] = '%.2f' % (percentage)

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


@app.route('/search')
def html_search():
    """
    Perform search on given input and redirect to correct search URL
    """
    query = request.args.get('q', '').strip()
    if not query:
        return redirect('/stats', code=302)

    database = Database()
    if database.get_workflows_with_prepid(query, page_size=1):
        return redirect('/stats?prepid=' + query, code=302)

    if database.get_workflows_with_output_dataset(query, page_size=1):
        return redirect('/stats?output_dataset=' + query, code=302)

    if database.get_workflows_with_input_dataset(query, page_size=1):
        return redirect('/stats?input_dataset=' + query, code=302)

    if database.get_workflows_with_campaign(query, page_size=1):
        return redirect('/stats?campaign=' + query, code=302)

    if database.get_workflows_with_type(query, page_size=1):
        return redirect('/stats?type=' + query, code=302)

    if database.get_workflows_with_processing_string(query, page_size=1):
        return redirect('/stats?processing_string=' + query, code=302)

    if database.get_workflows_with_request(query, page_size=1):
        return redirect('/stats?request=' + query, code=302)

    return redirect('/stats?workflow_name=' + query, code=302)


# Actual get method
def get_page(page=0):
    """
    Return a list of workflows based on url query parameters (if any)
    """
    database = Database()
    prepid = request.args.get('prepid')
    output_dataset = request.args.get('output_dataset')
    input_dataset = request.args.get('input_dataset')
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
            workflows = database.get_workflows_with_prepid(prepid,
                                                           page=page,
                                                           include_docs=True)
        elif output_dataset is not None:
            workflows = database.get_workflows_with_output_dataset(output_dataset,
                                                                   page=page,
                                                                   include_docs=True)
        elif input_dataset is not None:
            workflows = database.get_workflows_with_input_dataset(input_dataset,
                                                                  page=page,
                                                                  include_docs=True)
        elif campaign is not None:
            workflows = database.get_workflows_with_campaign(campaign,
                                                             page=page,
                                                             include_docs=True)
        elif workflow_type is not None:
            workflows = database.get_workflows_with_type(workflow_type,
                                                         page=page,
                                                         include_docs=True)
        elif processing_string is not None:
            workflows = database.get_workflows_with_processing_string(processing_string,
                                                                      page=page,
                                                                      include_docs=True)
        elif request_name is not None:
            workflows = database.get_workflows_with_request(request_name,
                                                            page=page,
                                                            include_docs=True)
        else:
            workflows = database.get_workflows(page=page,
                                               include_docs=True)

    if prepid is not None or output_dataset is not None or input_dataset is not None or request_name is not None:
        workflows = sorted(workflows,
                           key=lambda wf: '_'.join(wf.get('RequestName').split('_')[-3:-1]))

    return workflows


def run_flask():
    """
    Parse command line arguments and start flask web server
    """
    setup_console_logging()
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
