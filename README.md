## Script Purpose:
Find Interactive Hive queries that have been running for longer than the configured threshold, and kill them.

This script was designed to be run on HDInsight 3.6, Interactive Hive 2.1 clusters, and has been adapted to run on HDInsight 4.0, Interactive Hive 3.1 clusters. Two additional configuration steps are required for HDInsight 4.0 Interactive Hive clusters that are not required on HDInsight 3.6 clusters.

HDInsight 4.0 deploys Hive 3.1; which has a feature called Hive workload management.  Hive workload management includes Hive triggers. [Hive Triggers](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/hive-workload-commands/content/hive_create_trigger.html) can be used to kill queries that exceed a configured threshold if desired. As such, on HDInsight 4.0, Hive Triggers should be utilized in lieu of this script where possilbe.

## Overview:
The llap_query_watcher script is designed to be run on a periodic basis via a task scheduler like cron. 

The script supports both ESP and Standard cluster types.

The script was written and tested with Python 3.5.

When the script begins execution, it opens its configuration file, `conf/config.json`, to retrieve run-time settings and initialize its "state". 

After initialization is complete, the script calculates a time-range, or "window", of HIVE_QUERY Application Timeline Server (ATS) events it will process for the current run. The script consumes batches of HIVE_QUERY events from ATS, and looks for queries that have been executing for longer than the configured threshold.  If a query is found to have exceeded the configured threshold, and the user that submitted the query isn't a "whitelisted" user, the script "shells out" a beeline process to run a `kill query` command to terminate the long-running query.

When the script finishes execution, it saves the high-watermark (timestamp) of the most recent event processed to the aforementioned configuration file.  The next susbsequent execution of the script will consume the previous run's high-watermark value and use it as the lower bound of the window of ATS events to process.

## Installation Instructions:
The script requires the following configuration:
1. The script, llap_query_watcher.py, must be copied to the local drive of one of the cluster headnodes. It's probably easiest to download the zip from this git repo and extract it to the local drive. The script must be hosted on one of the cluster headnodes as it issues REST requests against "internal" cluster endpoints; not the cluster's public endpoint.

2. By default, the script's configuration file, `config.json`, should be stored in a subdirectory named `conf` that's created in the same folder where the llap_query_watcher script resides:
	```
	 /home/dansha/llap_watcher-master/llap_query_watcher.py
	 /home/dansha/llap_watcher-master/conf/config.json
	```

3. Run the linux chmod command to make the script executable by owner.  
	```
	chmod 764 llap_query_watcher.py
	```

4. The script is intended to be executed as a scheduled task, and has been tested with cron.  Use the crontab -e
command to edit the crontab of the logged on user (if appropriate), and add a line similar to the following that executes the script every 15 minutes:

	NOTE: 	The use of cd command to set the current working directory before launching the script.	This required as the script uses a relative path to load its configuration file config.json from the conf folder
	```
	# m h  dom mon dow   command
	*/15 * * * * cd /home/dansha/llap_watcher-master && ./llap_query_watcher.py
	```
5. The script makes REST requests to the cluster's Ambari Server service. To allow the script to authenticate with Ambari Server, update the `credentials` section of the `config.json` file. Set the `username` property to Ambari Admin user specified at cluster creation time.  The default value is "admin". Set the `password` parameter to the password of the Ambari Admin user. 
	```
	"credentials": {
	    "password": "<password>",
	    "username": "admin"
	}
	```
	You should configure Linux filesystem permissions to prevent unauthorized access to the `config.json` file. 

6. If the the cluster where the script is deployed is kerberized, there are the following additional requirements:

   6.a The script has a dependency on the requests_kerberos library. You must install the requests_kerberos module on the host where the script will be executed using the following command:
   ```
   pip3 install requests_kerberos
   ```

   6.b Select a domain account the script will utilize to kinit with the KDC, and update the `runner_kerberos_user` with the chosen domain account. The `runner_kerberos_user` can be found in the `runner_kerberos_config` section of the `conf/config.json` file:
	```
	"runner_kerberos_config": {
	    "runner_kerberos_user_keytab_location": "/home/user/user.keytab",
	    "runner_kerberos_user": "user@MYREALM.COM"
	}
	```

   6.c You must pre-create the required keytab. 
Use the Kerberos `ktutil` command to create a keytab for the `runner_kerberos_user`, and properly configure the file and folder permissions so the `runner_kerberos_user` can access the keytab file but no unauthorized users can.  
You can follow the instructions in the public [HDInsight documentation](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-faq#security-and-certificates):
	```
	ktutil
	ktutil: addent -password -p <username>@<DOMAIN.COM> -k 1 -e RC4-HMAC
	Password for <username>@<DOMAIN.COM>: <password>
	ktutil: wkt <username>.keytab
	ktutil: q
	```
	Now, update the `runner_kerberos_user_keytab_location` in the `runner_kerberos_config` section of `conf/config.json` with the full path to the keytab file.

	### Additional "service-side" configuration changes are required for HDInsight 4.0 clusters
	
	You can skip steps 7 and 8 for HDInsight 3.6 clusters.

7. Configuration changes to authorize use of `kill query` command

	On "Standard" Interactive Hive 3.1 clusters, only "admin" users are authorized to execute the `kill query` HQL command. The llap_query_watcher script runs the `kill query` command under the security context of the Ambari admin user configured in step 5 above. To add the user specified for `credentials.username` to the Hive `admin` role please take the following steps:  
	1. Log into your cluster's Ambari website 
	2. Open the Hive settings, and give "focus" to the "ADVANCED" tab       
	3. Navigate to the "Custom hive-site" section, and click "Add Property".  The "Add Property" dialog will open       
	4. To the "Key" field, add `hive.users.in.admin.role`       
	5. In the "Value" field, add the account configured for `credentials.username`. Again, this should be the Ambari admin user       
	6. Click the green "SAVE" button on the bottom right-hand side of the page, and add a note regarding your change to the "Save Configuration" dialog before clicking "SAVE" to dismiss the dialog.       
	7. Click "PROCEED ANYWAY".  Do not click "RESTART" to restart the Hive services until you complete step 8 below.

	NOTE: If the user configured as `credentials.username` is not added to the Hive `admin` role, attempts to kill queries will fail indicating the user is not authorized

   	On ESP clusters, `kill query` commands are executed under the security context of the `runner_kerberos_user` specified in the `config.json` file. The `runner_kerberos_user` must have `serviceadmin` permission assigned via a Ranger Hive policy in order to be authorized to issue the `kill query` command.  The HDInsight ESP cluster admin AD user is a member of the `all - hiveservice` role by default, and the `serviceadmin` permission is assigned to the `all - hiveservice` role.
	
	If the user configured as the `runner_kerberos_user` is not the HDInsight ESP cluster admin AD user, the `runner_kerberos_user` must be granted `serviceadmin` permissions. You can either add the `runner_kerberos_user` to the `all - hiveservice` role via the Ranger admin UI, or you can create a custom Hive role via the Ranger Admin UI that includes the `serviceadmin` Hive permission, and add the `runner_kerberos_user` to the newly created custom role.

**NOTE:** If the [Tez View](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-release-notes#support-tez-view-for-hdinsight-40) feature is available on your HDInsight 4.0 cluster, step 8 is unnecessary and doesn't need to be completed.

8. Configure Interactive Hive 3.1 to log HIVE_QUERY events to Application Timeline Server:

	Interactive Hive 3.1 is no longer configured to log HIVE_QUERY events to Application Timeline Server (ATS) as it was in Interactive Hive 2.1. This script requires HIVE_QUERY events to be logged to ATS to function properly. 

	To make the required configuration changes:
	1. Log into your cluster's Ambari website
	2. Open the Hive settings, and give "focus" to the "ADVANCED" tab
	3. Navigate to the "General" section, and locate the `hive.exec.pre.hooks` and `hive.exec.post.hooks` configuration settings. 
	4. Add `org.apache.hadoop.hive.ql.hooks.ATSHook` to **BOTH** `hive.exec.pre.hooks` and `hive.exec.post.hooks` by appending a single comma (",") directly after the existing entry, and then add `org.apache.hadoop.hive.ql.hooks.ATSHook` after the comma.  
	There should be *NO* whitespace between the comma, and `org.apache.hadoop.hive.ql.hooks.ATSHook`. 
	The `hive.exec.pre.hooks` and `hive.exec.post.hooks` values should both be configured as follows when you are finished making your edits:	
	`org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook,org.apache.hadoop.hive.ql.hooks.ATSHook`
	5. Click the green "SAVE" button on the bottom right-hand side of the page, and add a note regarding your change to the "Save Configuration" dialog before clicking "SAVE" to dismiss the dialog.
	6. Click "PROCEED ANYWAY".  
	7. Now click the orange "RESTART" button in the top right-hand corner of the page, and click "Restart All Affected" to restart all hive services and install the changes.

	If anything goes wrong, remember you can revert to the prior configuration by selecting the cluster configuration "Version" that precedes the changes you made.

## CONFIG.JSON configuration file:
```"runnerConfig": {
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
```

Parameter Definitions:

`ats_request_query_parameters` - Parameters relevant to REST requests made to ATS:
* `lookback_window_hours`: Determines how far back in time to begin searching for ATS HIVE_QUERY events. Currently only used for the first execution of the script on a given cluster
* `ats_request_batch_size`: The number of HIVE_QUERY events to consume for each ATS REST request
* `lookback_window_begin_timestamp`: Defines the lower-bound of the window of ATS HIVE_QUERY events to consume. Initial value is zero.Each execution of the script saves the most recent (highest) timestamp successfully processed by the prior run of the script

`logging_config` - Utility uses the python logging library to log progress messages to the console and a file
* `log_level`: 	minimum log level of messages logged by utility at runtime.  Valid values are DEBUG, INFO, WARN, ERROR
* `watcher_log_name`: Utility will use the TimedRotatingFileHandler logger to log messages to the file specified for this parameter
* `watcher_log_directory`: Directory created by the utility for log file storage
* `max_number_log_files`: Passed to TimedRotatingFileHandler for maximum number of log files to maintain on disk

`runner_kerberos_config` - Script will use these parameters to kinit on a Kerberized cluster so it can authenticate with cluster services

`credentials.username`, `credentials.password` - the user name of the Ambari admin user specified at cluster creation time, and admin user's associated password

`rest_performance_alert_threshold_seconds` - The round-trip response time for each REST request is compared with this integer alert threshold.  If the request took longer than threshold,
					   and alert like the following is written to the watcher log:
 					   2020-02-05 20:27:20,244 WARNING - [get] : <PERFORMANCE ALERT> REST request round-trip response time: [0:00:06.235511] exceeds the alert threshold of: [0:00:05] seconds!

`rest_request_retry_delay_seconds` - The number of seconds to delay between retry attempts for failed REST requests.  Used in method get_with_retry()
`rest_request_retry_count` - The number of times to retry a failed REST request before re-raising the exception to the caller.  Used in method get_with_retry()

`kill_query_threshold_seconds` - If the script finds a currently executing query that has been running longer than kill_query_threshold_seconds, it will shell out a beeline process and kill the query.

### Whitelist configuration
* When "whitelisting" is enabled, `whitelist`.`enabled` = `True`, an llap query will only be killed if two conditions are met:
  1. The query duration has exceeded the configured threshold of `kill_query_threshold_seconds`
  2. The user that the query is running under, is not one of the users specified in the `whiteslist`.`users` list.  Format of list is like that above.  One user on each line, specified as a double-quoted string.

* If whitelisting is disabled, only condition 1 above has to be met for a query to be killed

If whitelistng is enabled, and the script finds a query that exceeds the configured threshold, yet the user is a member of `whitelist`.`users`, the query will not be killed.

To disable the whitelisting feature, you must set `enabled` to `False`.

`sleep_seconds` - used in initialize_security_context() function between call to kdestroy and kinit

`headnodehost` Please leave this parameter set to its default value of "headnodehost".  Modifying this parameter will cause script execution to fail under certain scenarios.

Remaining parameters are self-explanatory.





