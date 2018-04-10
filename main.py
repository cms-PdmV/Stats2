from flask import Flask, render_template, request
from flask_restful import Api
from database import Database

app = Flask(__name__)
api = Api(app)


@app.route('/')
def index(name=None):
    database = Database()
    requests = database.get_all_requests()
    requests = list(filter(lambda x: len(x) > 1, requests))
    requests = list(filter(lambda x: x['TotalEvents'] != 0, requests))

    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')

    if prepid is not None:
        requests = list(filter(lambda x: x['PrepID'] == prepid, requests))

    if dataset is not None:
        requests = list(filter(lambda x: x['OutputDataset'] == dataset, requests))

    return render_template('index.html', requests=requests)


def run_flask():
    app.run(host='0.0.0.0',
            port=5000,
            debug=True,
            threaded=True)


if __name__ == '__main__':
    run_flask()
