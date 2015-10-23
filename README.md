# Spartkan

#### Your First Project

You can create a new project using:

	> sparktan quickstart my_project
	
	Creating your my_project sparktan project...
    create    my_project
    create    my_project/.gitignore
    create    my_project/requirements.txt
    create    my_project/config.json
    create    my_project/README.md
    create    my_project/main.py
    
	Done.

	Try running your project with:

		cd my_project
		sparktan run


#### Project Structure

filename	| description
----------- | --------------------------------
config.json | Configuration of the EMR cluster
requirements.txt |  requirements.txt of the virtual environment
main.py | Your awesome Spark script

#### Fetch info about existing cluster

	> sparktan list
	
	2015-10-23 11:02:58,694 [INFO] botocore.credentials - Found credentials in environment variables.
	2015-10-23 11:02:58,927 [INFO] botocore.vendored.requests.packages.urllib3.connectionpool - Starting new HTTPS connection (1): elasticmapreduce.us-east-1.amazonaws.com
	{u'Id': u'j-27KUKY8IY12Y0',
 	'MasterPrivateIpAddress': u'10.21.0.177',
 	'MasterPublicDnsName': u'ec2-54-172-110-197.compute-1.amazonaws.com',
 	u'Name': u'my_project',
 	u'NormalizedInstanceHours': 19200,
 	u'Status': {u'State': u'WAITING',
    	         u'StateChangeReason': {u'Message': u'Waiting for steps to run'},
        	     u'Timeline': {u'CreationDateTime': datetime.datetime(2015,10, 19, 10, 50, 3, 501000, tzinfo=tzlocal()),
                  u'ReadyDateTime': datetime.datetime(2015, 10, 19, 11, 6, 13, 23000, tzinfo=tzlocal())}}}
                  
#### Terminate your cluster
	> sparktan terminate j-27KUKY8IY12Y0

	
