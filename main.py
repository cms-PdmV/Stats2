"""
Module that starts webserver and has all it's endpoints
"""
import os
import json
import time
import argparse
import re
import logging
from flask import (
    Flask,
    render_template, 
    request, 
    make_response, 
    redirect,
    Response,
    jsonify
)
from flask_restful import Api
from couchdb_database import Database
from utils import setup_console_logging, get_unique_list, get_nice_size, comma_separate_thousands
from stats_update import StatsUpdate


app = Flask(__name__,
            static_folder='./html/static',
            template_folder='./html')
api = Api(app)

# Set up logging
setup_console_logging()

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
    page = 0
    workflows = []
    fetched = [{}]
    while len(fetched) > 0 and page < 100:
        fetched = get_page(page)
        workflows.extend(fetched)
        page += 1
        time.sleep(0.1)

    response = make_response(json.dumps(workflows, indent=2, sort_keys=True), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


def administrative_action() -> bool:
    """
    Check that a HTTP request is allowed to execute some administrative actions
    by checking roles provided in its headers.

    Returns:
        bool: True if the request provides at least one of the autorized roles to perform
            administrative actions, False otherwise.
    """
    authorized: list[str] = ["cms-pdmv-serv"]
    request_roles: list[str] = []
    roles_str: str = ""
    
    if request.headers.get("Adfs-Group"):
        roles_str = request.headers.get("Adfs-Group", "")
        roles_str = roles_str.replace(";", ",")
        request_roles = roles_str.strip().split(",")
    elif request.headers.get("Roles"):
        roles_str = request.headers.get("Roles", "")
        roles_str = roles_str.replace(";", ",")
        request_roles = roles_str.strip().split(",")

    auth_set: set[str] = set(authorized)
    roles_set: set[str] = set(request_roles)
    return bool(auth_set & roles_set)


@app.route(rule='/api/update', methods=["POST"])
def update_workflow() -> Response:
    error: dict[str, str] = {}
    
    # Check request is allowed
    authorized: bool = administrative_action()
    if not authorized:
        error = {"msg": "You are not allowed to perform this action"}
        response = jsonify(error)
        response.status_code = 403
        return response

    # Get the workflow name from query parameters
    workflow_name: str = request.args.get("workflow", "")
    if not workflow_name:
        error = {"msg": "Please provide the workflow name via 'workflow' query parameter"}
        response = jsonify(error)
        response.status_code = 400
        return response

    # Perform the update
    try:
        stats_update: StatsUpdate = StatsUpdate()
        stats_update.perform_update(workflow_name=workflow_name)
        
        result: dict[str, str] = {"msg": f"Workflow {workflow_name} has been updated successfully"}
        response = jsonify(result)
        response.status_code = 200
        return response
    except Exception as e:
        error_msg: str = (
            f"Unfortunately, there were issues updating workflow: {workflow_name}, there are described below:\n"
            f"{str(e)}"
        )
        error = {"error": error_msg}
        response = jsonify(error)
        response.status_code = 500
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


def get_service_type_and_name(workflow):
    """
    Return a tuple of service type and name for a given workflow
    """
    prepid = workflow.get('PrepID')
    if not prepid:
        return 'unknown', ''

    if matches_regex(prepid, '^ReReco-.*-[0-9]{5}$'):
        return 'rereco_machine', 'ReReco'

    if matches_regex(prepid, '^ReReco-.*$'):
        return 'rereco', ''

    if matches_regex(prepid, '^CMSSW_.*-[0-9]{5}$'):
        return 'relval_machine', 'RelVal'

    if matches_regex(prepid, '^CMSSW_.*'):
        return 'relval', ''

    if matches_regex(prepid, '^(task_)?[A-Z0-9]{3}-.*-[0-9]{5}$'):
        return 'mc', 'McM'

    return 'unknown', ''


def get_campaign_link(name, service):
    """
    Return a link to a campaign in a given service
    """
    if service == 'mc':
        return 'https://cms-pdmv-prod.web.cern.ch/mcm/campaigns?prepid=%s' % (name)
    if service == 'rereco_machine':
        return 'https://cms-pdmv-prod.web.cern.ch/rereco/subcampaigns?prepid=%s' % (name)
    if service == 'relval_machine':
        cmssw_version = name.split('__')[0]
        batch_name = name.split('__')[-1].split('-')[0]
        campaign_timestamp = name.split('-')[-1]
        return 'https://cms-pdmv-prod.web.cern.ch/relval/relvals?cmssw_release=%s&batch_name=%s&campaign_timestamp=%s' % (cmssw_version, batch_name, campaign_timestamp)

    return ''


def get_request_link(name, service):
    """
    Return a link to a request in a given service
    """
    if service == 'mc':
        return 'https://cms-pdmv-prod.web.cern.ch/mcm/requests?prepid=%s' % (name)
    if service == 'rereco_machine':
        return 'https://cms-pdmv-prod.web.cern.ch/rereco/requests?prepid=%s' % (name)
    if service == 'relval_machine':
        return 'https://cms-pdmv-prod.web.cern.ch/relval/relvals?prepid=%s' % (name)

    return ''


def get_campaign_links(name, service_type, service_name):
    """
    Return all links to a campaign in a given service
    """
    links = []
    if service_type in ('mc', 'relval_machine', 'rereco_machine'):
        links.append({'name': service_name,
                      'link': get_campaign_link(name, service_type)})

    links.append({'name': 'pMp',
                  'link': 'https://cms-pdmv-prod.web.cern.ch/pmp/historical?r=%s' % (name)})
    return links


def get_request_links(name, service_type, service_name):
    """
    Return all links to a request in a given service
    """
    links = []
    if service_type in ('mc', 'relval_machine', 'rereco_machine'):
        links.append({'name': service_name,
                      'link': get_request_link(name, service_type)})

    links.append({'name': 'pMp',
                  'link': 'https://cms-pdmv-prod.web.cern.ch/pmp/historical?r=%s' % (name)})
    return links


def get_time_diff(t1, t2):
    """
    Translate difference in seconds to days or hours or minutes or seconds
    """
    seconds = t2 - t1
    days = int(seconds / 86400)
    if days:
        return '%sd' % (days)

    seconds -= days * 86400
    hours = int(seconds / 3600)
    seconds -= hours * 3600
    minutes = int(seconds / 60)
    if hours:
        return '%sh %smin' % (hours, minutes)

    if minutes:
        return '%smin' % (minutes)

    seconds -= minutes * 60
    return '%ss' % (seconds)


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
    now = int(time.time())
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
                req['FirstStatus'] = status
                req['FirstStatusTime'] = update_time
                req['FirstStatusAgo'] = get_time_diff(first_transition['UpdateTime'], now)

            if 'Status' in last_transition and 'UpdateTime' in last_transition:
                status = last_transition['Status']
                update_time = time.strftime(datetime_format,
                                            time.localtime(last_transition['UpdateTime']))
                req['LastStatus'] = status
                req['LastStatusTime'] = update_time
                req['LastStatusAgo'] = get_time_diff(last_transition['UpdateTime'], now)

        req['LastUpdateAgo'] = get_time_diff(req['LastUpdate'], now)
        req['LastUpdate'] = time.strftime(datetime_format, time.localtime(req['LastUpdate']))
        req['Requests'] = get_unique_list(req.get('Requests', []))
        req['Campaigns'] = get_unique_list(req.get('Campaigns', []))
        service_type, service_name = get_service_type_and_name(req)
        # Links to external pages - McM, ReReco, RelVal, pMp
        attribute = 'request'
        if len(req['Requests']) == 0 and req.get('PrepID'):
            attribute = 'prepid'
            req['Requests'] = [req['PrepID']]

        req['Campaigns'] = [{'name': x,
                             'links': get_campaign_links(x, service_type, service_name)}
                            for x in req['Campaigns']]
        req['Requests'] = [{'name': x,
                            'attribute': attribute,
                            'links': get_request_links(x, service_type, service_name)}
                           for x in req['Requests']]

        calculated_datasets = []

        total_events = req.get('TotalEvents', 0)
        total_lumisections = req.get('TotalInputLumis', 0)
        for dataset in req['OutputDatasets']:
            new_dataset = {'Name': dataset,
                           'Events': 0,
                           'Lumis': 0,
                           'Type': 'NONE',
                           'CompletedPerc': '0.0',
                           'LumiCompletedPerc': '0.0',
                           'Datatier': dataset.split('/')[-1],
                           'Size': -1,
                           'NiceSize': '0B'}
            
            # Retrieve the most recent history entry for the current request
            history_entries = sorted(
                req['EventNumberHistory'],
                key=lambda entry: entry.get('Time', 0),
                reverse=True
            )
            for history_entry in history_entries:
                history_entry = history_entry['Datasets']
                if dataset in history_entry:
                    output_lumisections: int = history_entry[dataset].get('Lumis', 0)
                    new_dataset['Events'] = comma_separate_thousands(history_entry[dataset]['Events'])
                    new_dataset['Lumis'] = comma_separate_thousands(output_lumisections)
                    new_dataset['Type'] = history_entry[dataset]['Type']
                    new_dataset['Size'] = history_entry[dataset].get('Size', -1)
                    new_dataset['NiceSize'] = get_nice_size(new_dataset['Size'])
                    if total_events > 0:
                        percentage = history_entry[dataset]['Events'] / total_events * 100.0
                        new_dataset['CompletedPerc'] = '%.2f' % (percentage)
                    if total_lumisections > 0:
                        lumi_percentage = output_lumisections / total_lumisections * 100.0
                        new_dataset['LumiCompletedPerc'] = '%.2f' % (lumi_percentage)

                    break

            calculated_datasets.append(new_dataset)

        req['OutputDatasets'] = calculated_datasets
        req['TotalEvents'] = comma_separate_thousands(int(total_events))
        req['TotalInputLumis'] = comma_separate_thousands(int(total_lumisections))

        if 'RequestPriority' in req:
            req['RequestPriority'] = comma_separate_thousands(int(req['RequestPriority']))

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


@app.route('/update')
def html_update():
    """
    Update one workflow
    """
    wf_name = request.args.get('workflow_name', '').strip()
    if not wf_name:
        return redirect('/stats', code=302)

    stats_update = StatsUpdate()
    stats_update.update_one(wf_name)
    return redirect('/stats?workflow_name=' + wf_name, code=302)


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
    parser = argparse.ArgumentParser(description='Stats2')
    parser.add_argument('--port',
                        help='Port, default is 8001',
                        type=int,
                        default=8001)
    parser.add_argument('--host',
                        help='Host IP, default is 127.0.0.1',
                        default='127.0.0.1')
    parser.add_argument('--debug',
                        help='Run Flask in debug mode',
                        action='store_true')
    args = vars(parser.parse_args())
    port = args.get('port', None)
    host = args.get('host', None)
    debug = args.get('debug', False)
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        # Do only once, before the reloader
        pid = os.getpid()
        logging.info('PID: %s', pid)
        with open('stats.pid', 'w') as pid_file:
            pid_file.write(str(pid))

    app.run(host=host,
            port=port,
            debug=debug,
            threaded=True)


if __name__ == '__main__':
    run_flask()
