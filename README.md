# Stats2
## What is it?
Stats2 takes list of requests from *RequestManager2* and stores smaller (not all attributes) copy of all requests. It also takes nuber of open/done events from *dbs* and collects history for each dataset in all requests.
Main improvement over old Stats is that Stats2 fetch only changes of workflows since last update and fetches events only for these workflows which are currently in production. As a result, updates take about 10-20 minutes instead of 1-2 hours.
## No Javascript!
Stats2 has 0 javascript. However, it uses Jinja2 templates a lot.
## Console usage
Usually Stats2 should be viewed and controlled from web browser, but it  can be used as a python script as well:
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
Drop all databases (I don't know why this function is still here):
```
python3 stats_update.py --action drop
```
Note that update actions require two environment variables: `USERKEY` and `USERCRT` which should point to user GRID certificate and key files.

## Installation
### Install Python 3 and pip
Install Python 3.6 and pip3
```
yum install -y python36.x86_64
python3.6 -m ensurepip --default-pi
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
Campaigns (name:`campaigns`):
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
Output datasets (name:`outputDatasets`):
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
PrepIDs (name:`prepids`):
```
function (doc) {
  if (doc.PrepID) {
    emit(doc.PrepID, doc.RequestName);
  }
}
```
Request types (name:`types`):
```
function (doc) {
  if (doc.RequestType) {
    emit(doc.RequestType, doc.RequestName);
  }
}
```
Processing strings (name:`processingStrings`):
```
function (doc) {
  if (doc.ProcessingString && doc.ProcessingString.length > 0) {
    emit(doc.ProcessingString, doc.RequestName);
  }
}
```

### Install dependencies
Install flask and flask_restful
```
sudo python3.6 -m pip install flask
sudo python3.6 -m pip install flask_restful
```
### Clone Stats2
```
git clone https://github.com/justinasr/Stats2.git
```
## Running it
### Launch the website
Provide links to grid certificate files (for Update now function) and launch the main.py
```
export USERCRT=/.../user.crt.pem
export USERKEY=/.../user.key.pem
python3 main.py [--ssl_cert] [--ssl_key] [--port]
```
There are three available arguments when launching a website: `--ssl_cert`, `--ssl_key` and `--port`. If paths to certificates (certificate and key respectively) are provided in ssl_cert and ssl_key then website launches with ssl and port is set to 443. If not, then website without ssl is launched on port 80. Port settings can be overwritten with `--port`.
```
python3 main.py &
python3 main.py --ssl_cert /home/ssl.crt --ssl_key /home/ssl.key &
python3 main.py --ssl_cert /home/ssl.crt --ssl_key /home/ssl.key --port 8000 &
python3 main.py --port 8000 &
```
### Perform update
Provide the grid certificate and key (for cmsweb interaction) and start the initial update
```
export USERCRT=/.../user.crt.pem
export USERKEY=/.../user.key.pem
python3 stats_updater.py --action update
```
