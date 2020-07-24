Script Puprose:
Find Interactive Hive queries that have been running for longer than the configured threshold, and kill them.

Assumptions:
This script was designed to be run on HDInsight 3.6 Interactive Hive clusters. The script supports both ESP and Standard cluster types.

HDI 4.0 deploys Hive 3.x which has a feature called Hive Triggers that can be used to kill queries that exceed a given threshold if desired.  As such, this script was not tested on HDInsight 4.0.

Overview:
The llap_query_watcher script is designed to be run on a periodic basis via a task scheduler like cron. 

When the script begins execution, it opens its configuration file, conf/config.json, to retrieve run-time settings and initialize its "state". 

After initialization is complete, the script calculates a time-range, or "window", of HIVE_QUERY Application Timeline Server (ATS) events it will process for the current run. The script consumes batches of HIVE_QUERY events from ATS, and looks for queries that have been executing for longer than the configured threshold.  If a query is found to have exceeded the configured threshold, and the user that submitted the query isn't a "whitelisted" user, the script "shells out" a beeline process to run a "kill query" command to terminate the long-running query.

When the script finishes execution, it saves the high-watermark (timestamp) of the most recent event processed to the aforementioned configuration file.  The next susbsequent execution of the script will consume the previous run's high-watermark value and use it as the lower bound of the window of ATS events to process.

Installation Instructions:
The script requires the following configuration:
1. The script was written and tested with Python 3.5 on HDI 3.6 Interactive Hive clusters
2. The script, llap_query_watcher.py, must be copied to the local drive of one of the cluster headnodes. It's probably easiest to download the zip from this git repo and extract it to the local drive. The script must be hosted on one of the cluster headnodes as it issues REST requests against "internal" cluster endpoints; not the cluster's public endpoint.
3. By default, the cluster's configuration, config.json, should be stored in a subdirectory named "conf" that's created in the same folder where the llap_query_watcher script resides:

Example:
/home/dansha/llap_watcher-master/llap_query_watcher.py
/home/dansha/llap_watcher-master/conf/config.json

Edit the config.json file to allow for proper functioning of the script.  At a minimum, you'll need to specify the cluster's Ambari admin credentials in the credentials section of the config.json file. 

4. Run the linux chmod command to make the script executable by owner.  
Example:
chmod 764 llap_query_watcher.py

5. The script is intended to be executed as a scheduled task, and has been tested with cron.  Use the crontab -e
command to edit the crontab of the logged on user (if appropriate), and add a line similar to the following that executes the script every 15 minutes:
NOTE: 	The use of cd command to set the current working directory before launching the script. 
	This required as the script uses a relative path to load its configuration file config.json from the conf folder
#
# m h  dom mon dow   command
*/15 * * * * cd /home/dansha/llap_watcher-master && ./llap_query_watcher.py


6. If the the cluster where the script is deployed is kerberized, there are the following additional requirements:

6.a The script has a dependency on the requests_kerberos library. You must install the requests_kerberos module on the host where the script will be executed using the following command:
pip3 install requests_kerberos

6.b Select a domain account the script will utilize to kinit with the KDC, and add the name to the runner_kerberos_config in the config.json:
"runner_kerberos_config": {
       "runner_kerberos_user_keytab_location": "/home/user/user.keytab",
       "runner_kerberos_user": "user@MYREALM.COM"
}

6.c You must pre-create the required keytab. 
Use the Kerberos ktutil command to create a keytab for the runner_kerberos_user, and properly configure the file and folder permissions so the runner_kerberos_user can access the keytab file but no unauthorized users can.  
You can follow the instructions in the public HDInsight documentation:
[https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-faq#security-and-certificates]
ktutil
ktutil: addent -password -p <username>@<DOMAIN.COM> -k 1 -e RC4-HMAC
Password for <username>@<DOMAIN.COM>: <password>
ktutil: wkt <username>.keytab
ktutil: q

Now, update the "runner_kerberos_config" with the full path to the keytab file.

CONFIG.JSON configuration file:
    "runnerConfig": {
        "sleep_seconds": 1,
        "headnodehost": "headnodehost",
        "rest_request_retry_count": 5,
        "ambari_server_port": 8080,
        "yarn_rest_endpoint": "ws/v1/cluster",
        "logging_config": {
            "log_level": "DEBUG",
            "max_number_log_files": 7,
            "watcher_log_name": "llap_watcher.log",
            "watcher_log_directory": "llap_watcher_logs"
        },
        "runner_kerberos_config": {
            "runner_kerberos_user_keytab_location": "/home/user/user.keytab",
            "runner_kerberos_user": "user@REALM.COM"
        },
        "credentials": {
            "password": "<password>",
            "username": "admin"
        },
        "hiveserver2_thrift_port": 10001,
        "ambari_rest_protocol": "http",
        "kill_query_threshold_seconds": 3,
        "whitelist": {
            "users": [
                "admin",
                "anonymous"
            ],
            "enabled": "True"
        },
        "ats_rest_protocol": "http",
        "yarn_rm_port": 8088,
        "rest_request_retry_delay_seconds": 2,
        "ats_rest_endpoint": "ws/v1/timeline",
        "ats_http_port": 8188,
        "ambari_rest_endpoint": "api/v1/clusters",
        "yarn_rest_protocol": "http",
        "hdinsight_public_endpoint_port": 433,
        "rest_performance_alert_threshold_seconds": 5,
        "ats_request_query_parameters": {
            "ats_request_batch_size": 10,
            "lookback_window_begin_timestamp": 0,
            "lookback_window_hours": 120
        }
    }
}

Parameter Definitions:
ats_request_query_parameters - Parameters relevant to REST requests made to ATS:
	lookback_window_hours: 	Determines how far back in time to begin searching for ATS HIVE_QUERY events
				Currently only used for the first execution of the script on a given cluster
	ats_request_batch_size:	The number of HIVE_QUERY events to consume for each ATS REST request
	lookback_window_begin_timestamp: Defines the lower-bound of the window of ATS HIVE_QUERY events to consume
					 Initial value is zero.
					 Each execution of the script saves the most recent (highest) timestamp successfully
 					 processed by the prior run of the script

logging_config - Utility uses the python logging library to log progress messages to the console and a file
	log_level: 	minimum log level of messages logged by utility at runtime.  
			Valid values are DEBUG, INFO, WARN, ERROR
	watcher_log_name: Utility will use the TimedRotatingFileHandler logger to log messages to the file specified for this 
			  parameter
	watcher_log_directory: Directory created by the utility for log file storage
	max_number_log_files: Passed to TimedRotatingFileHandler for maximum number of log files to maintain on disk

runner_kerberos_config - Script will use these parameters to kinit on a Kerberized cluster so it can authenticate with cluster services

rest_performance_alert_threshold_seconds - The round-trip response time for each REST request is compared with this integer alert threshold.  If the request took longer than threshold,
					   and alert like the following is written to the watcher log:
 					   2020-02-05 20:27:20,244 WARNING - [get] : <PERFORMANCE ALERT> REST request round-trip response time: [0:00:06.235511] exceeds the alert threshold of: [0:00:05] seconds!

rest_request_retry_delay_seconds - The number of seconds to delay between retry attempts for failed REST requests.  Used in method get_with_retry()
rest_request_retry_count - The number of times to retry a failed REST request before re-raising the exception to the caller.  Used in method get_with_retry()

kill_query_threshold_seconds - If the script finds a currently executing query that has been running longer than kill_query_threshold_seconds, it will shell out a beeline process and kill the query.

whitelist configuration - When "whitelisting" is enabled, [whitelist][enabled] = True, an llap query will only be killed if two conditions are met:
	1. The query duration has exceeded the configured threshold of kill_query_threshold_seconds
	2. The user that the query is running under, is not one of the users specified in the [whiteslist][users] list.  Format of list is like that above.  One user on each line, specified as a double-quoted string.

	If whitelisting is disabled, only condition 1 above has to be met for a query to be killed

	If whitelistng is enabled, and the script finds a query that exceeds the configured threshold, yet the user is a member of [whitelist][users], the query will not be killed.

	To disable the whitelisting feature, you must set enabled to "False".  The "F" in "False" must be captialized since the value is cast to a Python bool by the script

sleep_seconds - used in initialize_security_context() function between call to kdestroy and kinit

headnodehost - 	Please leave this parameter set to its default value of "headnodehost".  
		Modifying this parameter will cause script execution to fail under certain scenarios.

Remaining parameters are self-explanatory.

