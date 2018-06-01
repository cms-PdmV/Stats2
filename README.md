# Stats2
## What is it?
Stats2 takes list of requests from *RequestManager2* and stores smaller (not all attributes) copy of all requests. It also takes nuber of open/done events from *dbs* and collects history for each dataset in all requests.
Main improvement over old Stats is that Stats2 fetch only changes since last update. As a result, updates take no more than few seconds and can be performed as frequently as desired.
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
Install Python 3.4 and pip3
```
sudo yum install python34.x86_64
sudo yum install python34-pip.noarch
```
### ~~Install mongodb
With the vim editor, create a .repo file for yum, the package management utility for CentOS:
```
sudo vim /etc/yum.repos.d/mongodb-org.repo
```
Then, visit the Install on Red Hat section of MongoDB’s documentation and add the repository information for the latest stable release to the file:
```
/etc/yum.repos.d/mongodb-org.repo
[mongodb-org-3.4]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/3.4/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-3.4.asc
```
Save and close the file.

~~We can install the mongodb-org package from the third-party repository using the yum utility.
```
sudo yum install mongodb-org
```
Next, start the MongoDB service with the systemctl utility:
```
sudo systemctl start mongod
```
### Install pymongo~~
```
sudo pip3 install pymongo
```
### Install dependencies
Install flask and flask_restful
```
sudo pip3 install flask
sudo pip3 install flask_restful
```
### Clone Stats2
```
git clone https://github.com/justinasr/Stats2.git
```
## Running it
### Launch the website
Provide the certificate and key and launch the website
```
export USERCRT=/.../user.crt.pem
export USERKEY=/.../user.key.pem
python3 main.py &
```
### Perform update
Provide the certificate and key and start the initial update
```
export USERCRT=/.../user.crt.pem
export USERKEY=/.../user.key.pem
python3 stats_updater.py --action update
```
