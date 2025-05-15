from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import requests
import json 
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NiFiAPI:
    def __init__(self, connection_id: str = None):
        """Initialize NiFi API client using Airflow Variable for connection settings"""
        # Get NiFi connection info from Airflow Variables or use default
        connection_id = connection_id or "nifi_default"
        try:
            connection_info = Variable.get(connection_id, deserialize_json=True)
            self.base_url = connection_info.get("base_url", "http://nifi-standalone:8080/nifi-api/")
            self.auth_type = connection_info.get("auth_type", "none")  # none, token, basic
            self.token = connection_info.get("token")
            self.username = connection_info.get("username")
            self.password = connection_info.get("password")
        except (KeyError, json.JSONDecodeError):
            logger.warning(f"Could not find or parse {connection_id} variable, using default settings")
            self.base_url = "http://nifi-standalone:8080/nifi-api/"
            self.auth_type = "none"
            self.token = None
            self.username = None
            self.password = None
        
        # Ensure base_url ends with a slash
        if not self.base_url.endswith('/'):
            self.base_url += '/'
            
        # Set up headers
        self.headers = {'Content-Type': 'application/json'}
        
        # Add authentication if configured
        if self.auth_type == "token" and self.token:
            self.headers['Authorization'] = f'Bearer {self.token}'
        
        logger.info(f"Initializing NiFi API client with base URL: {self.base_url}")
        
    def _make_request(self, method: str, endpoint: str, data=None, params=None):
        """Make HTTP request to NiFi API with error handling"""
        # Ensure endpoint doesn't start with a slash
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
            
        url = self.base_url + endpoint
        logger.info(f"Making {method} request to: {url}")
        
        try:
            # Prepare authentication for basic auth if needed
            auth = None
            if self.auth_type == "basic" and self.username and self.password:
                auth = (self.username, self.password)
            
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params, auth=auth)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data, auth=auth)
            elif method == 'PUT':
                response = requests.put(url, headers=self.headers, json=data, auth=auth)
            elif method == 'DELETE':
                response = requests.delete(url, headers=self.headers, auth=auth)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check if response is successful
            response.raise_for_status()
            
            # Check if response is empty
            if not response.content:
                return None
                
            # Try to parse as JSON
            return response.json()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text[:500]}...")
            
            # For 404 errors, we want to return None instead of raising
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
                return None
            raise
    
    def start_process_group(self, process_group_id: str):
        """Start all components in a process group"""
        try:
            data = {
                "id": process_group_id,
                "state": "RUNNING"
            }
            
            response = self._make_request('PUT', f'flow/process-groups/{process_group_id}', data)
            logger.info(f"Started process group {process_group_id}")
            return response
        except Exception as e:
            logger.error(f"Error starting process group {process_group_id}: {e}")
            return None
    
    def stop_process_group(self, process_group_id: str):
        """Stop all components in a process group"""
        try:
            data = {
                "id": process_group_id,
                "state": "STOPPED"
            }
            
            response = self._make_request('PUT', f'flow/process-groups/{process_group_id}', data)
            logger.info(f"Stopped process group {process_group_id}")
            return response
        except Exception as e:
            logger.error(f"Error stopping process group {process_group_id}: {e}")
            return None
    
    def get_process_group_status(self, process_group_id: str, recursive=True):
        """Get detailed status of a process group"""
        try:
            params = {'recursive': str(recursive).lower()}
            status = self._make_request('GET', f'flow/process-groups/{process_group_id}/status', params=params)
            aggregated_status = status['processGroupStatus']['aggregateSnapshot']
            
            return {
                'active_threads': aggregated_status.get('activeThreadCount', 0),
                'queued_count': aggregated_status.get('queuedCount', 0),
                'queued_bytes': aggregated_status.get('queuedContentSize', 0),
                'input_count': aggregated_status.get('inputCount', 0),
                'output_count': aggregated_status.get('outputCount', 0),
                'bytes_read': aggregated_status.get('bytesRead', 0),
                'bytes_written': aggregated_status.get('bytesWritten', 0),
                'errors': aggregated_status.get('errors', 0),
                'bulletin_count': aggregated_status.get('bulletinCount', 0)
            }
        except Exception as e:
            logger.error(f"Error getting process group status: {e}")
            return None
    
    def get_bulletins(self, process_group_id: str):
        """Get bulletins (error messages) for a process group"""
        try:
            bulletins = self._make_request('GET', f'flow/process-groups/{process_group_id}/bulletins')
            return bulletins
        except Exception as e:
            logger.error(f"Error getting bulletins for process group {process_group_id}: {e}")
            return None
    
    def get_processor_types(self, process_group_id: str):
        """Get all processor types in a process group"""
        try:
            response = self._make_request('GET', f'flow/process-groups/{process_group_id}/processors')
            processors = []
            
            if response and 'processors' in response:
                for processor in response['processors']:
                    processors.append({
                        'id': processor['component']['id'],
                        'name': processor['component']['name'],
                        'type': processor['component']['type'].split('.')[-1]
                    })
                    
            return processors
        except Exception as e:
            logger.error(f"Error getting processors for process group {process_group_id}: {e}")
            return []
    
    def get_connection_status(self, process_group_id: str):
        """Get status of all connections in a process group"""
        try:
            response = self._make_request('GET', f'flow/process-groups/{process_group_id}/connections')
            connections = []
            
            if response and 'connections' in response:
                for connection in response['connections']:
                    connections.append({
                        'id': connection['component']['id'],
                        'name': connection['component']['name'],
                        'source_name': connection['component']['source']['name'],
                        'destination_name': connection['component']['destination']['name'],
                    })
            
            return connections
        except Exception as e:
            logger.error(f"Error getting connections for process group {process_group_id}: {e}")
            return []
    
    def is_process_group_running(self, process_group_id: str):
        """Check if any processor in the process group is running"""
        status = self.get_process_group_status(process_group_id)
        
        if status is None:
            return False
        
        # Check if there are any active threads in the process group
        return status['active_threads'] > 0
    
    def get_all_components_details(self, process_group_id: str):
        """Get detailed information about all components in a process group"""
        try:
            # Get processors
            processors = self.get_processor_types(process_group_id)
            
            # Get connections
            connections = self.get_connection_status(process_group_id)
            
            # Get status
            status = self.get_process_group_status(process_group_id)
            
            # Get bulletins
            bulletins = self.get_bulletins(process_group_id)
            
            return {
                'processors': processors,
                'connections': connections,
                'status': status,
                'bulletins': bulletins
            }
        except Exception as e:
            logger.error(f"Error getting component details for process group {process_group_id}: {e}")
            return None
    
    def has_expected_terminal_processors(self, process_group_id: str):
        """Check if the process group has the expected terminal processors (ExecuteSQL or PutDatabaseRecord)"""
        processors = self.get_processor_types(process_group_id)
        
        # Check if the flow has at least one of the expected terminal processors
        terminal_processors = ['ExecuteSQL', 'PutDatabaseRecord']
        has_terminal = any(
            any(p_type in proc['type'] for p_type in terminal_processors)
            for proc in processors
        )
        
        return has_terminal
    
    def has_excessive_errors(self, process_group_id: str, max_errors=5):
        """Check if the process group has excessive errors"""
        status = self.get_process_group_status(process_group_id)
        
        if status is None:
            return False
        
        return status['errors'] > max_errors
    
    def wait_for_process_group_completion(self, process_group_id: str, check_interval: int = 10, 
                                         timeout: int = 300, max_errors: int = 5,
                                         stability_period: int = 30):
        """
        Wait for a process group to complete execution successfully
        
        Args:
            process_group_id: The ID of the process group
            check_interval: How often to check status (seconds)
            timeout: Maximum time to wait (seconds)
            max_errors: Maximum allowable errors before stopping
            stability_period: Time with no changes to confirm completion (seconds)
        """
        start_time = time.time()
        
        # First, check if flow has expected terminal processors
        has_terminal_processors = self.has_expected_terminal_processors(process_group_id)
        if not has_terminal_processors:
            logger.warning(f"Process group {process_group_id} does not have expected terminal processors")
        
        # Variables to track stability
        last_activity_time = time.time()
        last_status = None
        
        while time.time() - start_time < timeout:
            # Check if there are excessive errors
            if self.has_excessive_errors(process_group_id, max_errors):
                logger.error(f"Process group {process_group_id} has excessive errors (>{max_errors}). Stopping flow.")
                self.stop_process_group(process_group_id)
                return False
            
            # Get current status
            current_status = self.get_process_group_status(process_group_id)
            
            if current_status is None:
                logger.error(f"Could not get status for process group {process_group_id}")
                return False
            
            # Check if there is any activity (active threads or queued data)
            is_active = int(current_status['active_threads']) > 0 or int(current_status['queued_count']) > 0
            
            # Check for changes since last status check
            status_changed = False
            if last_status is not None:
                # Check if any of these metrics changed
                for key in ['input_count', 'output_count', 'bytes_read', 'bytes_written']:
                    if current_status[key] != last_status[key]:
                        status_changed = True
                        break
            
            # Update last status
            last_status = current_status
            
            # If there's activity or status changed, update the last activity time
            if is_active or status_changed:
                last_activity_time = time.time()
                logger.info(f"Process group {process_group_id} is active or had changes, resetting stability timer")
                
                # Log details about the current status
                logger.info(f"Status: Active threads={current_status['active_threads']}, "
                           f"Queued items={current_status['queued_count']}, "
                           f"Input={current_status['input_count']}, "
                           f"Output={current_status['output_count']}, "
                           f"Errors={current_status['errors']}")
            else:
                # No activity and no changes
                inactive_time = time.time() - last_activity_time
                logger.info(f"Process group {process_group_id} is inactive for {inactive_time:.1f} seconds")
                
                # If the flow has been stable (inactive) for the stability period
                if inactive_time >= stability_period:
                    # For flows that processed data (had input or output)
                    if current_status['input_count'] > 0 or current_status['output_count'] > 0:
                        if current_status['errors'] == 0:
                            logger.info(f"Process group {process_group_id} completed successfully")
                            return True
                        else:
                            logger.warning(f"Process group {process_group_id} completed but had {current_status['errors']} errors")
                            return False
                    else:
                        logger.warning(f"Process group {process_group_id} is inactive but did not process any data")
                        return False
            
            time.sleep(check_interval)
        
        logger.warning(f"Timeout reached while waiting for process group {process_group_id} to complete")
        return False


def run_nifi_process_group(**kwargs):
    """
    Run an existing NiFi process group
    
    Returns:
        True if successful
    """
    # Get the process group ID from Airflow Variables
    process_group_id = Variable.get("existing_process_group_id", default_var="your-process-group-id")
    
    # Get NiFi connection ID from Airflow Variables or use default
    nifi_connection_id = Variable.get("nifi_connection_id", default_var="nifi_default")
    
    # Get configuration parameters from Airflow Variables or use defaults
    check_interval = int(Variable.get("nifi_check_interval", default_var="10"))
    timeout = int(Variable.get("nifi_timeout", default_var="300"))
    max_errors = int(Variable.get("nifi_max_errors", default_var="5"))
    stability_period = int(Variable.get("nifi_stability_period", default_var="30"))
    
    logger.info(f"Starting NiFi process group: {process_group_id}")
    
    nifi = NiFiAPI(connection_id=nifi_connection_id)
    
    try:
        # Start the process group
        nifi.start_process_group(process_group_id)
        
        # Wait for process group to complete
        logger.info("Waiting for process group to complete...")
        success = nifi.wait_for_process_group_completion(
            process_group_id,
            check_interval=check_interval,
            timeout=timeout,
            max_errors=max_errors,
            stability_period=stability_period
        )
        
        if success:
            logger.info(f"Process group {process_group_id} completed successfully")
        else:
            logger.warning(f"Process group {process_group_id} did not complete successfully")
        
        # Store the success status in XCom
        kwargs['ti'].xcom_push(key='nifi_success', value=success)
        
        return success
        
    except Exception as e:
        logger.error(f"Error running process group: {e}")
        # Try to stop the process group to clean up
        try:
            nifi.stop_process_group(process_group_id)
        except Exception as cleanup_error:
            logger.error(f"Error stopping process group during cleanup: {cleanup_error}")
        
        # Re-raise the original exception
        raise


def run_spark_job(**kwargs):
    """
    Run a fake Spark job
    
    Returns:
        True if successful
    """
    logger.info("Starting Spark job...")
    
    # In a real scenario, this would submit a Spark job
    # For simulation, we'll just wait a bit
    time.sleep(5)
    
    logger.info("Spark job completed successfully")
    return True


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'simplified_nifi_spark',
    default_args=default_args,
    description='A simplified DAG to run an existing NiFi process group followed by a PySpark job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nifi', 'pyspark'],
) as dag:
    
    nifi_task = PythonOperator(
        task_id='run_nifi_process_group',
        python_callable=run_nifi_process_group,
        provide_context=True,
    )
    
    pyspark_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
        provide_context=True,
    )
    
    # Set task dependencies
    nifi_task >> pyspark_task
