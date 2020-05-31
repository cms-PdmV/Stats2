# Stats2
## What is it?
Stats2 takes list of requests from *RequestManager2* and stores smaller (not all attributes) copy of all requests. It also takes nuber of open/done events from *dbs* and collects history for each dataset in all requests.
Main improvement over old Stats is that Stats2 fetch only changes of workflows since last update and fetches events only for these workflows which are currently in production. As a result, updates take about 10-30 minutes instead of 1-2 hours.
## No Javascript*!
Stats2 website has absolutely no javascript! This leads to great compatibility and faster load times.
## Console usage
Usually Stats2 should be used from web browser, but it can be used as a python script as well:
Update all requests:
```
python3 stats_update.py --action update
```
Update one request (NAME is request name):
```
python3 stats_update.py --action update --name NAME
```
Preview one request (NAME is request name):
```
python3 stats_update.py --action see --name NAME
```

Note that update actions require two environment variables: `USERKEY` and `USERCRT` which should point to user GRID certificate and key files.

## Installation
### Install Python 3 and pip
Install Python 3.6 and pip3
```
yum install -y python36.x86_64
python3.6 -m ensurepip --default-pip
```
### CouchDB
##### Add CouchDB repo
Add:
```
[bintray--apache-couchdb-rpm]
name=bintray--apache-couchdb-rpm
baseurl=http://apache.bintray.com/couchdb-rpm/el$releasever/$basearch/
gpgcheck=0
repo_gpgcheck=0
enabled=1
```
To:
```
/etc/yum.repos.d/bintray-apache-couchdb-rpm.repo
```
or on puppet managed machines:
```
/etc/yum-puppet.repos.d/bintray-apache-couchdb-rpm.repo
```

##### Install CouchDB
```
sudo yum -y install epel-release
sudo yum -y install couchdb
```
CouchDB is installed to `/opt/couchdb` directory
##### Create views in CouchDB
Following views must be created in `requests` database, in `_designDoc` design document.

Campaigns (view name:`campaigns`):
```
function (doc) {
  if (doc.Campaigns) {
    addedCampaigns = {};
    var i;
    for (i = 0; i < doc.Campaigns.length; i++) {
      if (!(doc.Campaigns[i] in addedCampaigns)) {
        emit(doc.Campaigns[i], doc.RequestName);
        addedCampaigns[doc.Campaigns[i]] = true;
      }
    }
  }
}
```
Output datasets (view name:`outputDatasets`):
```
function (doc) {
  var i;
  if (doc.OutputDatasets) {
    for (i = 0; i < doc.OutputDatasets.length; i++) {
      emit(doc.OutputDatasets[i], doc.RequestName);
    }
  }
}
```
PrepIDs (view name:`prepids`):
```
function (doc) {
  if (doc.PrepID) {
    emit(doc.PrepID, doc.RequestName);
  }
}
```
Request types (view name:`types`):
```
function (doc) {
  if (doc.RequestType) {
    emit(doc.RequestType, doc.RequestName);
  }
}
```
Processing strings (view name:`processingStrings`):
```
function (doc) {
  if (doc.ProcessingString && doc.ProcessingString.length > 0) {
    emit(doc.ProcessingString, doc.RequestName);
  }
}
```
Requests (tasks) (view name:`requests`):
```
function (doc) {
  if (doc.Requests) {
    addedRequests = {};
    var i;
    for (i = 0; i < doc.Requests.length; i++) {
      if (!(doc.Requests[i] in addedRequests)) {
        emit(doc.Requests[i], doc.RequestName);
        addedRequests[doc.Requests[i]] = true;
      }
    }
  }
}
```

### Configure security
Stats2 CouchDB should be available to everyone to read, but no one, except admin should be allowed to update it.

In CouchDB settings: `require_valid_user` must be set to `false`.

In `requests` and `settings` databaes a new design document must be created:
```
{
  "_id": "_design/validate_write",
  "validate_doc_update": "function (newDoc, oldDoc, userCtx) { if (userCtx.roles.indexOf('_admin') === -1) throw( { forbidden : 'Only admins can modify the database.'} ); }"
}
```
This design document checks if user, who is trying to make changes, has `_admin` role.

### Install dependencies
Install flask and flask_restful
```
sudo python3.6 -m pip install flask
sudo python3.6 -m pip install flask_restful
```
### Clone Stats2
```
git clone https://github.com/cms-PdmV/Stats2.git
```
## Running it
### Launch the website
Launch the main.py
```
python3 main.py [--host] [--port] [--debug]
```
There are three available arguments when launching a website: `--host`, `--port` and `--debug`. Website by default is launched with host ip 127.0.0.1 on port 80. Host can be overwritten with `--host`. Port can be overwritten with `--port`. Parameter `--debug` launches flask server in debug mode.
```
python3 main.py &
python3 main.py --host 0.0.0.0 --port 8000 &
python3 main.py --debug
```

### Perform update
Provide paths to grid certificate and key files (for cmsweb interaction) and authorization string and start the update. Basic authentication header consists of words Basic and base64 encoded "username:password" value, for example: "Basic dXNlcjpwYXNzd29yZA==".

```
export USERCRT=/.../user.crt.pem
export USERKEY=/.../user.key.pem
export STATS_DB_AUTH_HEADER="Basic base64encodedvalue"
python3 stats_updater.py --action update
```

### Database and view compaction
Periodic database compaction must be performed to prevent host of running out of space:
```
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/settings/_compact -H "Authorization: Basic $STATS_DB_AUTH_HEADER"

curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/campaigns -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/outputDatasets -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/prepids -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/processingStrings -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/requests -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_compact/_designDoc/types -H "Authorization: Basic $STATS_DB_AUTH_HEADER"

curl -s -k -H "Content-Type: application/json" -X POST http://localhost:5984/requests/_view_cleanup -H "Authorization: Basic $STATS_DB_AUTH_HEADER"
```

