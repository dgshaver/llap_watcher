Script Puprose:
Find Interactive Hive queries that have been running for longer than the configured threshold, and kill them.

Assumptions:
This script was designed to be run on HDInsight 3.6 Interactive Hive clusters. The script supports both ESP and Standard cluster types.

HDI 4.0 deploys Hive 3.x which has a feature called Hive Triggers that can be used to kill queries that exceed a given threshold if desired.  As such, this script was not tested on HDInsight 4.0.

Overview:
The llap_query_watcher script is designed to be run on a periodic basis via a task scheduler like cron. 

When the script begins execution, it opens its configuration file, config/config.json, to retrieve run-time settings and initialize its "state". After initialization is complete, the script calculates a time-range, or "window", of Application Timeline Server (ATS) events it will process for the current run. The script consumes batches of HIVE_QUERY events from ATS, and looks for queries that have been executing for longer than the configured threshold.  If a query is found to have exceeded the configured threshold, the script "shells out" a beeline process to run a "kill query" command to terminate the long-running query.

When the script finishes execution, it saves the high-watermark (timestamp) of the most recent event processed to the aforementioned configuration file.  The next susbsequent execution of the script will consume the previous run's high-watermark value and use it as the lower bound of the window of ATS events to process.

Installation Instructions:
The script requires the following configuration:
1. The script was written and tested with Python 3.5 on HDI 3.6 Interactive Hive clusters
2. The script, llap_query_watcher.py, must be copied to the local drive of one of the cluster headnodes. REST requests issues by the script target internal cluster endpoints.
3. By default, the cluster's configuration, config.json, should be stored in a subdirectory named "conf" that's created in the same folder where the llap_query_watcher script resides:

Example:
/home/dansha/scripts/llap_query_watcher.py
/home/dansha/scripts/conf/config.json

4. The script is intended to be executed as a schedule task, and has been tested with cron.  Use the crontab -e
command to edit the crontab of the logged on user (if appropriate), and add a line similar to the following that executes the script every 15 minutes:
# m h  dom mon dow   command
*/15 * * * * /home/dansha/llap_query_watcher.py


5. If the the cluster where the script is deployed is kerberized, there are the following additional requirements:
5.a You must install the requests_kerberos module on the host where the script will be executed using the following command:
pip3 install requests_kerberos
5.b Select a domain account the script will utilize to kinit with the KDC, and name to the runner_kerberos_config in the config.json:
"runner_kerberos_config": {
       "runner_kerberos_user_keytab_location": "/home/sshuser/dansha.keytab",
       "runner_kerberos_user": "dansha"
}
5.c Use the Kerberos ktutil command to create a keytab for the runner_kerberos_user, and properly configure the file and folder permissions so the runner_kerberos_user can access the keytab file but no unauthorized users can.  You can follow the instructions in the public HDInsight documentation:
[https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-faq#security-and-certificates]
ktutil
ktutil: addent -password -p <username>@<DOMAIN.COM> -k 1 -e RC4-HMAC
Password for <username>@<DOMAIN.COM>: <password>
ktutil: wkt <username>.keytab
ktutil: q

Now, update the runner_kerberos_config with the full path to the keytab file.

CONFIG.JSON configuration file:
{
    "runnerConfig": {
        "ats_request_query_parameters": {
            "lookback_window_hours": 120,
            "ats_request_batch_size": 10,
            "lookback_window_begin_timestamp": 1580927381495
        },
        "logging_config": {
            "log_level": "DEBUG",
            "watcher_log_name": "llap_watcher.log",
            "max_number_log_files": 7,
            "watcher_log_directory": "llap_watcher_logs"
        },
        "headnodehost": "headnodehost",
        "runner_kerberos_config": {
            "runner_kerberos_user_keytab_location": "/home/sshuser/dansha.keytab",
            "runner_kerberos_user": "dansha"
        },
	"ambari_rest_endpoint": "api/v1/clusters",	
	"ambari_rest_protocol": "http",
	"ambari_server_port": 8080,
        "ats_rest_protocol": "http",
        "ats_rest_endpoint": "ws/v1/timeline",
	"ats_http_port": 8188,
        "yarn_rest_protocol": "http",
        "yarn_rest_endpoint": "ws/v1/cluster",
        "yarn_rm_port": 8088,
        "hiveserver2_thrift_port": 10001,
        "rest_performance_alert_threshold_seconds": 5,
        "rest_request_retry_delay_seconds": 2,
        "rest_request_retry_count": 5,
        "kill_query_threshold_seconds": 3,
        "sleep_seconds": 1
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

kill_query_threshold_seconds - If the script finds a currently executing query that has been running longer than kill_query_threshold_seconds, it will shell out a beeling process and kill the query.

sleep_seconds - used in initialize_security_context() function between call to kdestroy and kinit

Remaining parameters are self-explanatory.

