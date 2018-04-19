from flask import Flask, render_template, request
from flask_restful import Api
from database import Database
from utils import setup_console_logging

app = Flask(__name__)
api = Api(app)


@app.route('/')
@app.route('/<int:page>')
def index(page=0):
    database = Database()
    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')
    campaign = request.args.get('campaign')
    request_name = request.args.get('request_name')

    if request_name is not None:
        requests = [database.get_request(request_name)]
    else:
        if prepid is not None:
            requests = database.query({'PrepID': prepid}, page)
        elif dataset is not None:
            requests = database.query({'OutputDatasets': dataset}, page)
        elif campaign is not None:
            requests = database.query({'Campaign': campaign}, page)
        else:
            requests = database.query(page=page)

    return render_template('index.html',
                           requests=requests,
                           page=page,
                           total_requests=database.get_count_of_requests(),
                           query=request.query_string.decode('utf-8'))


def run_flask():
    setup_console_logging()
    app.run(host='0.0.0.0',
            port=5000,
            debug=True,
            threaded=True)


if __name__ == '__main__':
    run_flask()
