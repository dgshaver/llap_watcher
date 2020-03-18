#!/usr/bin/python3.5
import json
import sys
import traceback
import os
import time
from datetime import datetime, timedelta
import requests
try:
    from requests_kerberos import HTTPKerberosAuth
except:
    print('{0} WARNING - [llap_query_watcher] : failed to import Python library: requests_kerberos' \
        .format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]))
    print('{0} WARNING - [llap_query_watcher] : if HDI cluster utilizes Kerberos authentication, script execution will fail' \
        .format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]))
    print('{0} WARNING - [llap_query_watcher] : execution continuing ...' \
        .format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]))
import socket
from subprocess import check_output, CalledProcessError, STDOUT, Popen, PIPE
import shlex
import logging
from logging.handlers import TimedRotatingFileHandler
from enum import Enum
from socket import getfqdn, gethostname

# Logger 
_logger = None
# Cluster admin user and password. 
SLEEP_SECONDS = None
# Store runtime configuration in RUNNER_CONFIG so it can be re-written at shutdown 
RUNNER_CONFIG = None
# HDI_CLUSTER configuration.  Stores instantiation of HDINsigtCluster class
HDI_CLUSTER = None

class HDInsightCluster:
    def __init__(self
            ,ambari_admin_user
            ,ambari_admin_user_password 
            ,ambari_server_port
            ,yarn_rm_service_port
            ,ats_port
            ,hiveserver2_thrift_port
            ,ambari_rest_endpoint
            ,ambari_rest_protocol
            ,yarn_rest_endpoint
            ,yarn_rest_protocol
            ,ats_rest_endpoint
            ,ats_rest_protocol
            ,headnodehost
            ,kill_query_threshold_seconds):
        self.ambari_admin_user = ambari_admin_user
        self.ambari_admin_user_password = ambari_admin_user_password
        self.ambari_server_port = ambari_server_port
        self.yarn_rm_service_port = yarn_rm_service_port
        self.ats_port = ats_port 
        self.hiveserver2_thrift_port = hiveserver2_thrift_port
        self.ambari_rest_endpoint = ambari_rest_endpoint
        self.ambari_rest_protocol = ambari_rest_protocol
        self.yarn_rest_endpoint = yarn_rest_endpoint
        self.yarn_rest_protocol = yarn_rest_protocol
        self.ats_rest_endpoint = ats_rest_endpoint
        self.ats_rest_protcol = ats_rest_protocol
        self.headnodehost = headnodehost
        self.kill_query_threshold_seconds = kill_query_threshold_seconds
        # Initialized by method discover_headnodehost_fqdn()
        # runner_host-fqdn - FQDN of node where the llap_query_watcher script is executing
        self.runner_host_fqdn = None
        self.headnodehost_fqdn = None
        # Intialized by method discover_hdi_cluster_dns_name()
        self.hdi_cluster_dns_name = None
        self.hdi_cluster_public_endpoint = None
        # Intialized by method discover_hdi_cluster_security_type
        self.hdi_cluster_security_protocol = None
        # Intialized by get_llap_start_timestamp()
        self.llap_service_start_time = None
        self.llap_service_elapsed_time = None
        # Initialized by make_jdbc_connection_string()
        self.hivesever2_jdbc_connection_string = None
        # Initialized by discover_ats_execution_host()
        self.ats_execution_host_fqdn = None
        self.hiveserver2_interactive_execution_host_fqdn = None
        super().__init__()
    
        self.authentication_protocol = {"NONE": "NONE", "KERBEROS": "KERBEROS"}
            
    def discover_headnodehost_fqdn(self):
        _logger.info('Discovering HDInsight cluster headnode FQDN ...')
        self.runner_host_fqdn = getfqdn(gethostname())
        self.headnodehost_fqdn= getfqdn(self.headnodehost)
        _logger.info('script execution host is: {0}'.format(self.runner_host_fqdn))
        _logger.info('cluster headnodehost FQDN is: {0}'.format(self.headnodehost_fqdn))

    def discover_hdi_cluster_dns_name(self):
        _logger.info('Discovering cluster DNS name ...')
        # Ambari doesn't support Kerberos, so use ambari admin account to authenticate
        url = '{0}://{1}:{2}/{3}'.format(self.ambari_rest_protocol, self.headnodehost_fqdn, self.ambari_server_port, self.ambari_rest_endpoint)
        response = get(url, self.ambari_admin_user, self.ambari_admin_user_password)
        body = response.json()
        self.hdi_cluster_dns_name = body['items'][0]['Clusters']['cluster_name']
        self.hdi_cluster_public_endpoint = '{0}.azurehdinsight.net'.format(self.hdi_cluster_dns_name)
        _logger.info('Cluster DNS name is: {0}'.format(self.hdi_cluster_dns_name))

    def discover_hdi_cluster_security_type(self):
        _logger.info('Discovering cluster authentication method ...')
        # Ambari doesn't support Kerberos, so use ambari admin account to authenticate
        security_args = '?fields=Clusters/security_type'
        url = '{0}://{1}:{2}/{3}/{4}'.format(self.ambari_rest_protocol, self.headnodehost_fqdn, self.ambari_server_port, self.ambari_rest_endpoint, security_args)
        response = get(url, self.ambari_admin_user, self.ambari_admin_user_password)
        body = response.json()
        self.hdi_cluster_security_protocol =  body['items'][0]['Clusters']['security_type']
        _logger.info('Cluster: {0} configured to use security protocol: {1}'.format(self.hdi_cluster_dns_name, self.hdi_cluster_security_protocol))

    """
    On ESP clusters, ATS only runs on hn0 so get hostname for use in routing REST requests to the service
    """
    def discover_service_execution_host(self, service_name, component_name):
        _logger.info('Discovering execution host for {0}:{1}'.format(service_name, component_name))
        services_args = '{0}/services/{1}/components/{2}'.format(self.hdi_cluster_dns_name, service_name, component_name)
        url = '{0}://{1}:{2}/{3}/{4}'.format(self.ambari_rest_protocol, self.headnodehost_fqdn, self.ambari_server_port, self.ambari_rest_endpoint, services_args)
        response = get(url, self.ambari_admin_user, self.ambari_admin_user_password)
        body = response.json()
        # On HDI 3.6 ESP clusters, ATS always runs on hn0
        if body['ServiceComponentInfo']['component_name'] == component_name \
            and body['ServiceComponentInfo']['total_count'] == 1:
                execution_host_name = body['host_components'][0]['HostRoles']['host_name']
        else:
            # If number of installed instances is > 1, ATS should be running on headnodehost
            execution_host_name = self.headnodehost_fqdn
        _logger.info('Execution host for {0}:{1} is {2}'.format(service_name, component_name,execution_host_name))

        return(execution_host_name)
    
    """
    Discover location of services that may not be running on headnodehost 
    """
    def discover_service_execution_hosts(self):
        self.ats_execution_host_fqdn = self.discover_service_execution_host('YARN', 'APP_TIMELINE_SERVER')
        self.hiveserver2_interactive_execution_host_fqdn = self.discover_service_execution_host('HIVE', 'HIVE_SERVER_INTERACTIVE')

    """
    Hive JDBC connection string format is different for ESP and non-ESP clusters.  
    This method builds connection string appropriate to the authentication method in use Kerberos or none
    """
    def make_jdbc_connection_string(self):
        _logger.info("Constructing jdbc connection string for cluster {0} ...".format(self.hdi_cluster_dns_name))
        HIVE_CONFIGURATION_PARAMS = '{0}/configurations?type=hive-site&tag=TOPOLOGY_RESOLVED'.format( self.hdi_cluster_dns_name)
        url = '{0}://{1}:{2}/{3}/{4}'.format(self.ambari_rest_protocol, self.headnodehost_fqdn, self.ambari_server_port, self.ambari_rest_endpoint, HIVE_CONFIGURATION_PARAMS)
        response = get(url, self.ambari_admin_user, self.ambari_admin_user_password)
        body = response.json()
        # If cluster is kerberized
        if self.authentication_protocol['KERBEROS'] == self.hdi_cluster_security_protocol:
            hiveserver2_authentication_principal = body['items'][0]['properties']['hive.server2.authentication.kerberos.principal']
            self.hivesever2_jdbc_connection_string = \
                'jdbc:hive2://{0}:{1}/default;principal={2};auth-kerberos;transportMode=http' \
                .format(self.hiveserver2_interactive_execution_host_fqdn, self.hiveserver2_thrift_port, hiveserver2_authentication_principal)
        else:
            # hive_zookeeper_quorum = body.items[0]['properties']['hive.zookeeper.quorum']
            self.hivesever2_jdbc_connection_string = \
                'jdbc:hive2://{0}:{1}/;transportMode=http'.format(self.hiveserver2_interactive_execution_host_fqdn,self.hiveserver2_thrift_port)
               
    """
    Returns the HS2 JDBC connection string 
    """
    def get_jdbc_connection_string(self):
        return(self.hivesever2_jdbc_connection_string)
    
    """
    This method retrieves the starttime timestamp for the llap daemon.  
    Value is used on calculation of the beginning timestamp for ATS REST requests for a given run of the watcher
    Makes sure we don't wast time querying ATS events for queries that could no longer be running
    """
    def discover_llap_start_timestamp(self):
        _logger.info('Discovering LLAP service start time ...')
        # constants used by REST request and response processing
        LLAP_APPLICATION_NAME = 'llap0'
        YARN_QUEUE_NAME = 'llap'
        YARN_APPLICATION_STATE_RUNNING = 'RUNNING'
        LLAP_YARN_APPLICATION_TYPE = 'org-apache-slider'

        # REST request parameters
        parameters = {'queue': YARN_QUEUE_NAME, 'applicationTypes': LLAP_YARN_APPLICATION_TYPE}
        url = '{0}://{1}:{2}/{3}/apps'.format(self.yarn_rest_protocol, self.headnodehost_fqdn, self.yarn_rm_service_port, self.yarn_rest_endpoint)
        authentication_protocol = 'KERBEROS' if self.hdi_cluster_security_protocol == 'KERBEROS' else 'NONE'

        try:
            response = get(url, self.ambari_admin_user, self.ambari_admin_user_password, authentication_protocol, parameters)
        except Exception as ex:
            _logger.error('Attempt to retrieve LLAP start timestamp failed with error:\n{0}'.format(ex))
            _logger.error('The kinit for configured \'runner_user\' may have failed, or the cluster: {0} may not be a Hive LLAP cluster'.format(self.hdi_cluster_dns_name))
            # Re-throw the exception to shut the watcher down
            raise

        body = response.json()
        for application in body['apps']['app']:
            if application['state'] == YARN_APPLICATION_STATE_RUNNING and application['name'] == LLAP_APPLICATION_NAME:
                # YARN uses timestamps with millisecond preicison
                self.llap_service_start_time =  application['startedTime']
                self.llap_service_elapsed_time = application['elapsedTime']
        _logger.debug('llap start timestamp is: {0} - llap daemon execution duration is: {1}'.format(self.llap_service_start_time,self.llap_service_elapsed_time))

    def get_llap_service_start_timestamp(self):
        return(self.llap_service_start_time)

    def is_kerberized(self):
        return(self.hdi_cluster_security_protocol == self.authentication_protocol['KERBEROS'])

    def fetch_ats_events2(self, window_begin_timestamp=None, window_end_timestamp=None, batch_size=None):
        # Define window size for ATS REST request  
        if window_begin_timestamp is None and window_end_timestamp is None:
            # This request will retrieve the max timestamp
            parameters = {'limit': batch_size}
        elif window_begin_timestamp is None and window_end_timestamp > 0:
            parameters = {'limit': batch_size, 'windowEnd': window_end_timestamp}
        else:
            parameters = {'limit': batch_size, 'windowStart': window_begin_timestamp, 'windowEnd': window_end_timestamp}

        # Build ATS request url
        url = '{0}://{1}:{2}/{3}/HIVE_QUERY_ID'.format(self.ats_rest_protcol, self.ats_execution_host_fqdn, self.ats_port, self.ats_rest_endpoint)
        authentication_protocol = 'KERBEROS' if self.hdi_cluster_security_protocol == 'KERBEROS' else 'NONE'
        # Issue REST request to retrieve events from ATS using the parameters defined above
        response = get_with_retry(url, self.ambari_admin_user, self.ambari_admin_user_password, authentication_protocol, parameters)

        body = response.json()
        llap_queries = body['entities']
        _logger.debug('Fetched: {0} HIVE_QUERY events from ATS'.format(len(llap_queries)))
        # Store enumerated HIVE_QUERY events in list
        hive_query_events = []
        # The value of ats_request_window_end_timestamp is returned to caller
        ats_request_window_end_timestamp = 0
        hsi_query = {}
        for llap_query in llap_queries:
            hsi_query['query_complete_timestamp'] = None
            query_submission_timestamp_seconds = 0
            ats_request_window_end_timestamp = llap_query['starttime'] - 1
            hsi_query['query_id'] = llap_query['entity']
            # Each query has a max of two events [QUERY_SUBMITTED and QUERY_COMPLETED]
            for event in llap_query['events']:
                # If we encounter a QUERY_COMPLETED event, break as the query has finished execution
                if event['eventtype'] == 'QUERY_COMPLETED':
                    hsi_query['query_complete_timestamp'] = event['timestamp']//1000
                if event['eventtype'] == 'QUERY_SUBMITTED':
                    # Capture query submission timestamp and convert to seconds
                    hsi_query['query_start_timestamp'] = event['timestamp']//1000
            # The requestuser is the user that the request is running under, and is implemented as a list (not sure why)
            request_users = []
            for user in llap_query['primaryfilters']['requestuser']:
                request_users.append(user)
            if len(request_users) > 0:
                hsi_query['request_users'] = request_users
            _logger.debug('QueryID: {0} starttime: {1} request_users: {2}'.format(hsi_query['query_id'], ats_request_window_end_timestamp, request_users))
            hive_query_events.append(hsi_query.copy())
            hsi_query.clear()
        
        return(ats_request_window_end_timestamp, hive_query_events)

"""
Wrapper for requests GET calls
"""
def get(url, username, password, authentication_method='NONE', parameters=None):
    # If a REST request exceeds the alert_threshold_seconds, a WARN message will be logged
    alert_threshold_seconds = get_rest_performance_alert_threshold_seconds()
    # Configure the correct authentication mechanism
    auth=HTTPKerberosAuth() if authentication_method == 'KERBEROS' else (username,password)
    if parameters is None:
        response = requests.get(url, auth=auth)
    else:
        response = requests.get(url, auth=auth, params=(parameters))
    performance_alert_threshold_seconds = timedelta(seconds=alert_threshold_seconds)
    if response.elapsed > performance_alert_threshold_seconds:
        _logger.warn('<PERFORMANCE ALERT> REST request round-trip response time: [{0}] exceeds the alert threshold of: [{1}] seconds!' \
            .format(response.elapsed, performance_alert_threshold_seconds))
    if (requests.codes.ok != response.status_code):
        response.raise_for_status()
    return response

"""
Wrapper for GET REST calls that allows a configurable number of retries
"""
def get_with_retry(url, username, password, authentication_method='NONE', parameters=None):
    retry_count = get_rest_request_retry_count()
    retry_delay_seconds = get_rest_request_retry_delay_seconds()
    response = None
    STOP = retry_count+1
    for i in range(1,STOP):
        try:
            if parameters is not None:
                response = get(url, username, password, authentication_method, parameters)
            else:
                response = get(url, username, password, authentication_method)
            break
        except Exception as ex:
            _logger.error('REST request failed with error:\n {0}'.format(ex))
            # If retry failed retry_count times, send the exception back to the caller
            if i == retry_count:
                raise
            time.sleep(retry_delay_seconds)
            _logger.info('Initiating retry {0} of {1} ...'.format(i, retry_count))
            continue
    return(response)

"""
Execute command in linux sub-process
"""
def executeCmd(cmd):
    RETURN_CODE_SUCCESS = 0
    process = Popen(shlex.split(cmd),stdin=PIPE,stdout=PIPE,stderr=PIPE)
    stdout, stderr = process.communicate()
    # Depend on process' exit code to determine success or failure
    if process.returncode != RETURN_CODE_SUCCESS:
        _logger.error('process execution failed!  Exit code: {0}'.format(process.returncode))
        raise Exception(stderr)
    
"""
Check if a request_user is in the list of whitelist users:
config.json -> whitelist.users
"""
def is_whitelist_user(request_users):
    whitelist_users = get_whitelist_users()
    for request_user in request_users:
        if request_user in whitelist_users:
            return(True)
    return(False)

"""
This function considers two factors in determining whether a query should be killed:
1.  Does the query's current duration exceed the configured threshold?
    Subtracts the starttime from the current system time, and returns a boolean 
    indicating whether the task has been running for longer than kill_threshold seconds
    starttime - timestamp in seconds
2.  If "whitelist" is enabled, only kill a query that meets condition 1 if the query if
    a. The whitelist feature is enabled
    b. The request_user is not a member of the configured whitelist specified in config.json
"""
def should_kill(starttime, kill_threshold, request_users):
    # Is this a whitelisted request?
    kill_query = False if bool(is_whitelisting_enabled) and is_whitelist_user(request_users) else True

    # Determine whether query exceeds the configured kill_threshold
    current_time_seconds = int(time.time())
    duration_in_seconds = current_time_seconds - starttime
    _logger.debug('whitelisted: {0} current_time: {1} start_time: {2} query_duration: {3} kill_threshold_seconds: {4}' \
    .format(not kill_query, current_time_seconds,starttime,duration_in_seconds,kill_threshold))

    return(duration_in_seconds >= kill_threshold and kill_query)


"""
Launch beeline process to each query in queries_to_kill
"""
def kill_query(queries_to_kill):
    jdbc_connection_string = HDI_CLUSTER.get_jdbc_connection_string()
    # This parameter will be ignored on Standard cluster as there is no Hive security configured there
    user = get_runner_kerberos_user()
    BEELINE_LAUNCH_CMD = "beeline -u '{0}' -n {1} -e {2}"
    for query_id in queries_to_kill:
        cmds = 'kill query \'{0}\'; '.format(query_id)
        kill_cmd = BEELINE_LAUNCH_CMD.format(jdbc_connection_string, user, shlex.quote(cmds))
        _logger.info('Killing query: {0}'.format(query_id))
        executeCmd(kill_cmd)
    
"""
Make output directory for 
"""
def make_output_dir():
    watcher_output_directory = get_watcher_log_directory()
    dirs = os.listdir()
    # Check if output directory already exists
    if watcher_output_directory not in dirs:
        # If not, create it
        os.mkdir(watcher_output_directory)
    
    return(watcher_output_directory)

"""
Configure logging 
The runner will log to both the console and a text file
Logging-level can be set to a value supported by logging library: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
Please reference: https://docs.python.org/3/library/logging.html?highlight=logging#levels 
"""
def configure_logging(log_level):
    global _logger
    # Define format of messages logged by watcher
    CONSOLE_LOGGER_FORMAT = FILE_LOGGER_FORMAT = '%(asctime)-15s %(levelname)s - [%(funcName)s] : %(message)s'
    
    # Create the logger
    _logger = logging.getLogger('LLAP_WATCHER_LOGGER')
    # Convert string representation of logging level retrieved from the config.json to numeric equivalent exposed by logging library
    # and set the logLevel
    console_log_level = file_log_level = getattr(logging, log_level.upper())
    _logger.setLevel(console_log_level)
 
    # create console handler and set level to default logging level
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(CONSOLE_LOGGER_FORMAT)
    # add formatter to console_handler
    console_handler.setFormatter(console_formatter)
    # Set logging level
    console_handler.setLevel(console_log_level)
    # add console_handler to logger
    _logger.addHandler(console_handler)

    # Creat file log handler, but first create an output directory and construct log file name to configure logger with
    output_dir = make_output_dir()
    # Construct log file name
    llap_watcher_log = get_watcher_log_name()
    max_number_log_files = get_max_number_log_files()
    # create file handler
    file_handler = TimedRotatingFileHandler(os.path.join(output_dir,llap_watcher_log) \
        ,when='midnight', utc=True, backupCount=max_number_log_files)
    # Set logging level for execution log file to DEBUG
    file_handler.setLevel(file_log_level)
    file_formatter = logging.Formatter(FILE_LOGGER_FORMAT)
    file_handler.setFormatter(file_formatter) 
    _logger.addHandler(file_handler)  

def save_runner_config(config_file_path=None):
    global RUNNER_CONFIG
    if config_file_path is None:
        config_file_path = "conf/config.json" 
    _logger.info('Saving runner configuration to: {0}'.format(config_file_path))
    with open(config_file_path,'w',) as f:
        json.dump(RUNNER_CONFIG,f)

def get_lookback_window_begin_timestamp():
    _logger.debug('event_window_beginning_timestamp: [{0}] retrieved from runner configuration' \
        .format(RUNNER_CONFIG['runnerConfig']['ats_request_query_parameters']['lookback_window_begin_timestamp']))
    return(RUNNER_CONFIG['runnerConfig']['ats_request_query_parameters']['lookback_window_begin_timestamp'])

def set_lookback_window_begin_timestamp(lookback_window_begin_timestamp):
    _logger.info('Setting lookback_window_begin_timestamp with value: {0} into runner configuration'.format(lookback_window_begin_timestamp))
    RUNNER_CONFIG['runnerConfig']['ats_request_query_parameters']['lookback_window_begin_timestamp'] = lookback_window_begin_timestamp

def get_lookback_window_hours():
    return(RUNNER_CONFIG['runnerConfig']['ats_request_query_parameters']['lookback_window_hours'])

def get_ats_request_batch_size():
    return(RUNNER_CONFIG['runnerConfig']['ats_request_query_parameters']['ats_request_batch_size'])

def get_runner_kerberos_user():
    return(RUNNER_CONFIG['runnerConfig']['runner_kerberos_config']['runner_kerberos_user'])

def get_kerberos_keytab_location():
    return(RUNNER_CONFIG['runnerConfig']['runner_kerberos_config']['runner_kerberos_user_keytab_location'])

def get_sleep_seconds():
    return(RUNNER_CONFIG['runnerConfig']['sleep_seconds'])

def get_rest_request_retry_count():
    return(RUNNER_CONFIG['runnerConfig']['rest_request_retry_count'])

def get_rest_request_retry_delay_seconds():
    return(RUNNER_CONFIG['runnerConfig']['rest_request_retry_delay_seconds'])

def get_hsi_query_kill_threshold():
    return(RUNNER_CONFIG['runnerConfig']['kill_query_threshold_seconds'])
def get_whitelist_users():
    return(RUNNER_CONFIG['runnerConfig']['whitelist']['users'])

def is_whitelisting_enabled():
    return(RUNNER_CONFIG['runnerConfig']['whitelist']['enabled'])

def get_rest_performance_alert_threshold_seconds():
    return(RUNNER_CONFIG['runnerConfig']['rest_performance_alert_threshold_seconds'])

def get_max_number_log_files():
    return(RUNNER_CONFIG['runnerConfig']['logging_config']['max_number_log_files'])

def get_watcher_log_directory():
    return(RUNNER_CONFIG['runnerConfig']['logging_config']['watcher_log_directory'])

def get_watcher_log_name():
    return(RUNNER_CONFIG['runnerConfig']['logging_config']['watcher_log_name'])

def get_log_level():
    return(RUNNER_CONFIG['runnerConfig']['logging_config']['log_level'])

def kdestroy():
    try:
        _logger.info('Clearing cached Kerberos credentials ...')
        executeCmd('kdestroy')
    except Exception as ex:
        # If kerberos auth cache is empty, kdestroy will raise an error ... eat the exception
        _logger.warn('Attempt to clear cached Kerberos credentials failed. This is expected if credential cache was empty', exc_info=True)

def kinit(user, keytab_file):
    # try:
    _logger.info('Calling kinit as user: {0}'.format(user))
    cmd = 'kinit -kt {0} {1}'.format(keytab_file, user)
    executeCmd(cmd)

"""
This function will first call kdestroy to delete any cached security context for the user passed in via the "user" parameter, 
and then call kinit
"""
def initialize_security_context(user, keytab_file):
    sleep_seconds = get_sleep_seconds()
    kdestroy()
    time.sleep(sleep_seconds)
    kinit(user, keytab_file)

"""
Initialize query watcher state from config file
"""
def init(config_file_path = None):
    global HDI_CLUSTER, RUNNER_CONFIG
    if config_file_path is None:
        config_file_path = "conf/config.json" 
    with open(config_file_path,'r',) as f:
        RUNNER_CONFIG = json.load(f)

        # Configure logging based on log_level supplied in config.json
        configure_logging(get_log_level())
        _logger.info('### Beginning Execution ###')
        _logger.info('Logging configured to log level: {0}'.format(get_log_level()))

        _logger.info('Beginning script initialization ...')
        _logger.info('Loading configuration settings from: {0}'.format(config_file_path))

        HDI_CLUSTER = HDInsightCluster(RUNNER_CONFIG['runnerConfig']['credentials']['username'] 
            ,RUNNER_CONFIG['runnerConfig']['credentials']['password'] 
            ,RUNNER_CONFIG['runnerConfig']['ambari_server_port'] 
            ,RUNNER_CONFIG['runnerConfig']['yarn_rm_port'] 
            ,RUNNER_CONFIG['runnerConfig']['ats_http_port']
            ,RUNNER_CONFIG['runnerConfig']['hiveserver2_thrift_port']
            ,RUNNER_CONFIG['runnerConfig']['ambari_rest_endpoint']
            ,RUNNER_CONFIG['runnerConfig']['ambari_rest_protocol']
            ,RUNNER_CONFIG['runnerConfig']['yarn_rest_endpoint']
            ,RUNNER_CONFIG['runnerConfig']['yarn_rest_protocol']
            ,RUNNER_CONFIG['runnerConfig']['ats_rest_endpoint']
            ,RUNNER_CONFIG['runnerConfig']['ats_rest_protocol']
            ,RUNNER_CONFIG['runnerConfig']['headnodehost']
            ,RUNNER_CONFIG['runnerConfig']['kill_query_threshold_seconds'] )    
        
        HDI_CLUSTER.discover_headnodehost_fqdn()

        HDI_CLUSTER.discover_hdi_cluster_dns_name()

        # Find locations of LLAP and ATS services as they are not guaranteed to run on headnodehost
        HDI_CLUSTER.discover_service_execution_hosts()
      
        # Get cluster security type ['KERBEROS','Basic']
        HDI_CLUSTER.discover_hdi_cluster_security_type()

        # If cluster is kerberized, must have a TGT to authenticate with YARN and ATS endpoints
        if HDI_CLUSTER.is_kerberized():
            # Must be an AD user sync'd to AAD DS
            user = get_runner_kerberos_user()
            # should be full path to keytab
            keytab = get_kerberos_keytab_location()
            # kinit to generate Kerberos security context - requires keybab file for 'user' script is running under
            initialize_security_context(user, keytab)

        # Get starttime of LLAP service
        HDI_CLUSTER.discover_llap_start_timestamp()

        # Fetch zk quorum from Ambari and construct HS2 JDBC connection string
        HDI_CLUSTER.make_jdbc_connection_string()

    _logger.info('Script initialization complete ...')


"""
This function gets the current date and time using the datetime.utcnow() function and subtracts the specified
number of hours from it.
This *may* serve as the lower-bound of the HIVE_QUERY event window that will be consumed by the script from ATS
This value is currently only calculated for the very first run of the script on a given HDI 3.6 LLAP cluster
Subsequent runs use the maximum timestamp processed by the prior run for the beginWindow timestamp.  Otherwise, there would likely
be "holes" in the event windows processed.
If it's desired to always bound processing by the loockback window hours parameter, the script can be changed to accomodate that
"""
def calc_lookback_window_begin_timestamp(hours_to_process):
    hours = timedelta(hours=hours_to_process)
    end_time_utc = datetime.utcnow() - hours
    _logger.debug('Calculated intial request window boundaries. Input value for lookback_window_hours: {0}'.format(hours_to_process))
    _logger.debug('Calulated window boudaries are - windowStart: {0} (UTC) windowEnd: {1} (UTC)' \
        .format(end_time_utc.strftime('%Y-%m-%d %H:%M:%S'),(end_time_utc + hours).strftime('%Y-%m-%d %H:%M:%S')))
    #YARN timestamps are millisecond precision.  Multiiply by 1000 to convert to milliseconds, then discard factional remainder
    return(int(end_time_utc.timestamp()*1000))

"""
Each time this script begins execution, it has to calculate a time-window that bounds that HIVE_QUERY events the script
will process.
The high-watermark is the most recent HIVE_QUERY event when the script begins execution
The low-watermark or oldest timestamp to consider must be chosen subject to the following considerations:
- If the previous high watermark timestamp value saved from the prior run is > the lookback window value, use the high watermarkvalues:
- The high watermark processed by the last run
"""
def get_ats_query_window_begin_timestamp():
    # The very first time the script runs against a given ATS store, we don't want to process every hive query event known to the store.
    # Instead, the script first calculates a lookback timestamp to serve as the lower bound of the *intial* ATS event processing window.
    # The "ats_request_query_parameters":"lookback_window_hours" parameter from config.json is used to calculate the timestamp
    # NOTE: If desired, the logic can be modified to always choose the minimum of the lookback_window_hours and the last processed timestamp if desired
    event_window_beginning_timestamp = get_lookback_window_begin_timestamp()
    
    if event_window_beginning_timestamp == 0:
        event_window_beginning_timestamp = calc_lookback_window_begin_timestamp(get_lookback_window_hours())

        # Now, get llap start timestamp. 
        # This is to avoid unnecessary calls to ATS for events that precede the current llap instance's starttime 
        llap_start_time = HDI_CLUSTER.get_llap_service_start_timestamp()

        # If starttime of calculated event window precedes the start of the llap daemon, use llap starttime 
        if event_window_beginning_timestamp < llap_start_time:
            _logger.info('windowStart boundary: {0} preceeds start timestamp of llap daemon: {1}' \
                .format(event_window_beginning_timestamp, llap_start_time))
            _logger.info('Adjusting beginWindow boundary from: {0} to {1}'.format(event_window_beginning_timestamp, llap_start_time))
            event_window_beginning_timestamp = llap_start_time
    
    return(event_window_beginning_timestamp)

def calculate_ats_end_window_timestamp():
    # ATS returns HIVE_QUERY events in descending order. 
    # It's envisioned this script will be scheduled as a CRON task
    # Each time the script runs, it needs to calculate a window (beginTimestamp and endTimestamp) for the events it will process
    # process

    # Since ATS returns HIVE_QUERY events in descending sequence, we'll start with the most recent HIVE_QUERY event known to ATS.
    # The simplest way to do this is to request a batch of HIVE_QUERY events with a batch size of 1 with no windowEnd values. 
    # This request Will return current max timestamp for HIVE_QUERY events
    # The returned timestamp will be saved to config file to remember the most recent event processed by the script, and will be used
    # as the lower bound of the HIVE_QUERY event window to process on the next subsequent run
    ats_events_processed_high_water_mark = ats_query_window_end_timestamp = get_ats_events(batch_size=1)

    return(ats_events_processed_high_water_mark, ats_query_window_end_timestamp)

def get_ats_request_window_timestamps():
    # Initialize locals 
    ats_events_processed_high_water_mark = ats_query_window_end_timestamp = 0
    # Now, calculate or retrieve the timestamp that defines the lower-bound or beginning of the 
    # window of ATS events of type HIVE_QUERY to process
    ats_query_window_begin_timestamp = get_ats_query_window_begin_timestamp()
    # Now fetch the most recent HIVE_QUERY event timestamp, and use it for two things:
    # 1. endWindow timestamp for first request
    # 2. Save as the highest timestamp/high water mark processed by this run of the script: ats_events_processed_high_water_mark 
    ats_events_processed_high_water_mark, ats_query_window_end_timestamp = calculate_ats_end_window_timestamp()

    # If the attempt to fetch the HIVE_QUERY event from ATS with the most recent timestamp returns None, the ATS database is empty
    if ats_events_processed_high_water_mark > 0: 
        # Log window size
        _logger.debug('Processing HIVE_QUERY events between ats_query_window_begin_timestamp: {0} and ats_events_processed_high_water_mark: {1}' \
            .format(ats_query_window_begin_timestamp,ats_events_processed_high_water_mark))
        _logger.info('Processing Hive queries started between {0} and {1}' \
            .format(datetime.fromtimestamp(ats_query_window_begin_timestamp//1000).strftime('%Y-%m-%d %H:%M:%S'), \
                    datetime.fromtimestamp(ats_query_window_end_timestamp//1000).strftime('%Y-%m-%d %H:%M:%S')))
    else:
        # When we attempted to fetch the timestamp of the most recent HIVE_QUERY event from ATS, no events were returned
        _logger.info('Currently, there are no HIVE_QUERY events to process')
        _logger.info("Exiting ...")
    
    return(ats_events_processed_high_water_mark, ats_query_window_end_timestamp, ats_query_window_begin_timestamp)


def kill_hsi_queries(hsi_queries):
    # hsi_queries is list of dictionary objects; each with the following format:
    # hsi_query['query_id']:    The HSI queryID in string format.  
    #                           This is passed to the hive kill query command
    # hsi_query['query_complete_timestamp']:  If this field is set to None, the query is likely active since no QUERY_COMPLETE event
    #                                        was found for it by HDI_CLUSTER.fetch_ats_events()
    # hsi_query['query_start_timestamp']:   Begin timestamp found in QUERY_SUBMITTED event fetched from ATS 
    #                                       This value is used to calculate if the query has exceeded the configured threshold;
    #                                       "kill_query_threshold_seconds" loaded from the config.json file
    # hsi_query['request_users']: User(s) the request was run under/for
    # Retrieve kill query threshold from RUNNER_CONFIG
    kill_threshold = get_hsi_query_kill_threshold()
    for hsi_query in hsi_queries:
        if hsi_query['query_complete_timestamp'] is not None:
            continue
        if should_kill(hsi_query['query_start_timestamp'], kill_threshold, hsi_query['request_users']):
            # Sending as a list object for now.  Had intended to implement batching in kill method
            kill_query([hsi_query['query_id']])

def get_ats_events(window_begin_timestamp=None, window_end_timestamp=None, batch_size=None):
    _logger.debug('PARAMETERS: window_begin_timestamp: [{0}] window_end_timestamp: [{1}] batch_size: [{2}]' \
        .format(window_begin_timestamp, window_end_timestamp, batch_size))

    # Represents the minimum timestamp processed in the current batch. The timestamps are returned from ATS in descending order
    ats_request_window_end_timestamp = 0
    ats_request_fetch_window_size = get_ats_request_batch_size()
    
    # Allow batch size to be overridden by value supplied from function parameter list
    request_batch_size = batch_size if batch_size is not None else ats_request_fetch_window_size
    _logger.debug('Fetch HIVE_QUERY events from ATS in batches of {0} events'.format(request_batch_size))    

    # Fetch batch of HIVE_QUERY events within window
    hive_query_events = []
    ats_request_window_end_timestamp, hive_query_events = \
    HDI_CLUSTER.fetch_ats_events2(window_begin_timestamp=window_begin_timestamp \
        , window_end_timestamp=window_end_timestamp \
        , batch_size=request_batch_size)

    # Queries to kill are collected into a list as ats event batches are processed
    kill_hsi_queries(hive_query_events)

    _logger.debug('Returning ATS_REQUEST_WINDOW_END_TIMESTAMP: {0}'.format(ats_request_window_end_timestamp))
    # Return starttime timestamp of last event retrieved
    # It's possible that the last batch could be an empty batch.  
    # If so, ATS_REQUEST_WINDOW_END_TIMESTAMP == 0 and loop in run_Watcher will still terminate 
    return(ats_request_window_end_timestamp)

def run_watcher():
    _logger.info('Launching LLAP watcher ...')
    
    try:
        # Initialize locals 
        ats_events_processed_high_water_mark = ats_query_window_end_timestamp = ats_query_window_begin_timestamp = 0

        ats_events_processed_high_water_mark \
        , ats_query_window_end_timestamp \
        , ats_query_window_begin_timestamp = get_ats_request_window_timestamps()

        # If the begin and end timestamps are equal, no new HIVE_QUERY events have been written to ATS since the last run
        if ats_query_window_begin_timestamp == ats_query_window_end_timestamp:
            _logger.info('There are no new HIVE_QUERY events to process since the last run ...')
            _logger.info('Timestamp for last HIVE_QUERY event processed by the prior run is: {0}'.format(ats_query_window_begin_timestamp))
            _logger.info('Timestamp of the most recent HIVE_QUERY event known to ATS is: {0}'.format(ats_query_window_end_timestamp))
            
        # NOTE: The last batch processed may have up to (batch_size - 1) events that were processed by the prior script execution
        #while (ats_query_window_begin_timestamp <= ats_query_window_end_timestamp):
        while (ats_query_window_end_timestamp > ats_query_window_begin_timestamp):
            ats_query_window_end_timestamp = \
                get_ats_events(window_end_timestamp=ats_query_window_end_timestamp)

        _logger.info('Script execution is complete.  ats_query_window_begin_timestamp: [{0}] ats_query_window_end_timestamp: [{1}]' \
            .format(ats_query_window_begin_timestamp, ats_query_window_end_timestamp))
    finally:
        # Check if the entire event window was processed
        if ats_query_window_begin_timestamp >= ats_query_window_end_timestamp:
            # If the entire window was successfully processed, save the largest value processed to config file to be used as begin_timestamp for next run
            set_lookback_window_begin_timestamp(ats_events_processed_high_water_mark)
        else:
            # If script failed before processing entire window, start next script execution with the last event successfully processed
            set_lookback_window_begin_timestamp(ats_query_window_begin_timestamp)
        
        # Save changes to RUNNER_CONFIG configuration to disk for next run
        save_runner_config()
        
if __name__ == "__main__":
    try:
        init()
        run_watcher()
    except Exception as ex:
        _logger.error('Script execution failed with a fatal exception ...', exc_info=True)


    






  




