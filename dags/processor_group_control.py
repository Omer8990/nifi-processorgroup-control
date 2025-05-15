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
    # This way of checking if it finishes is only checking if there are 0 active threads, 
    def is_process_group_running(self, process_group_id: str):
        """Check if any processor in the process group is running"""
        try:
            status = self._make_request('GET', f'flow/process-groups/{process_group_id}/status')
            aggregated_status = status['processGroupStatus']['aggregateSnapshot']
            
            # Check if there are any active threads in the process group
            return aggregated_status['activeThreadCount'] > 0
        except Exception as e:
            logger.error(f"Error checking if process group is running: {e}")
            # Assume it's not running if we can't check
            return False
    
    def get_process_group_status(self, process_group_id: str):
        """Get detailed status of a process group"""
        try:
            status = self._make_request('GET', f'flow/process-groups/{process_group_id}/status')
            aggregated_status = status['processGroupStatus']['aggregateSnapshot']
            
            return {
                'active_threads': aggregated_status.get('activeThreadCount', 0),
                'queued_count': aggregated_status.get('queuedCount', 0),
                'input_count': aggregated_status.get('inputCount', 0),
                'output_count': aggregated_status.get('outputCount', 0),
                'bytes_read': aggregated_status.get('bytesRead', 0),
                'bytes_written': aggregated_status.get('bytesWritten', 0),
                'errors': aggregated_status.get('errors', 0)
            }
        except Exception as e:
            logger.error(f"Error getting process group status: {e}")
            return None

    def is_process_group_completed_successfully(self, process_group_id: str, prev_status=None):
        """
        Enhanced method to check if a process group completed successfully with no errors
        
        This implementation:
        1. Tracks changes in input/output counts to detect progress
        2. Considers a flow successful when:
        - No active threads remain
        - No errors are present
        - Either input/output counts > 0 (data was processed)
        - OR input/output counts haven't changed in consecutive checks (processing stalled)
        """
        status = self.get_process_group_status(process_group_id)
        
        if status is None:
            return False
        
        # Check if there are no active threads (no more processing happening)
        no_active_threads = status['active_threads'] == 0
        
        # Check if there are no errors
        no_errors = status['errors'] == 0
        
        # Check if data was processed
        processed_data = status['input_count'] > 0 or status['output_count'] > 0
        
        # If we have previous status, check if counts changed
        status_unchanged = False
        if prev_status:
            input_unchanged = status['input_count'] == prev_status['input_count']
            output_unchanged = status['output_count'] == prev_status['output_count']
            queue_unchanged = status['queued_count'] == prev_status['queued_count']
            status_unchanged = input_unchanged and output_unchanged and queue_unchanged
        
        # Success is when:
        # 1. No active threads (not processing)
        # 2. No errors
        # 3. Either data was processed OR status hasn't changed from previous check
        return no_active_threads and no_errors and (processed_data or status_unchanged)

    def wait_for_process_group_completion(self, process_group_id: str, check_interval: int = 5, timeout: int = 300):
        """Enhanced wait method that tracks status changes between checks"""
        start_time = time.time()
        prev_status = None
        unchanged_count = 0
        
        while time.time() - start_time < timeout:
            # Get current status
            status = self.get_process_group_status(process_group_id)
            
            if status is None:
                logger.warning(f"Could not get status for process group {process_group_id}")
                time.sleep(check_interval)
                continue
            
            # Check if there are no active threads
            if status['active_threads'] == 0:
                # Check if the completion was successful
                if self.is_process_group_completed_successfully(process_group_id, prev_status):
                    logger.info(f"Process group {process_group_id} completed successfully")
                    
                    # Log final stats
                    logger.info(f"Final stats - Input: {status['input_count']}, " 
                            f"Output: {status['output_count']}, "
                            f"Queued: {status['queued_count']}, "
                            f"Errors: {status['errors']}")
                    return True
                
                # Check if status is unchanged between checks
                if prev_status:
                    input_unchanged = status['input_count'] == prev_status['input_count']
                    output_unchanged = status['output_count'] == prev_status['output_count']
                    queue_unchanged = status['queued_count'] == prev_status['queued_count']
                    
                    if input_unchanged and output_unchanged and queue_unchanged:
                        unchanged_count += 1
                        
                        # If stats haven't changed for 3 consecutive checks and no active threads,
                        # we consider the flow complete
                        if unchanged_count >= 3:
                            logger.info(f"Process group {process_group_id} appears complete - "
                                    f"no activity for {unchanged_count} consecutive checks")
                            
                            # Check for errors before declaring success
                            if status['errors'] > 0:
                                logger.warning(f"Process completed with {status['errors']} errors")
                                return False
                            return True
                    else:
                        # Reset counter if stats changed
                        unchanged_count = 0
                
            else:
                # Reset unchanged count if there are active threads
                unchanged_count = 0
                    
            # Store current status for next comparison
            prev_status = status
            
            logger.info(f"Process group status - Active Threads: {status['active_threads']}, "
                    f"Input: {status['input_count']}, "
                    f"Output: {status['output_count']}, "
                    f"Queued: {status['queued_count']}")
            
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
    
    logger.info(f"Starting NiFi process group: {process_group_id}")
    
    nifi = NiFiAPI(connection_id=nifi_connection_id)
    
    try:
        # Start the process group
        nifi.start_process_group(process_group_id)
        
        # Wait for process group to complete
        logger.info("Waiting for process group to complete...")
        success = nifi.wait_for_process_group_completion(process_group_id)
        
        if success:
            logger.info(f"Process group {process_group_id} completed successfully")
        else:
            logger.warning(f"Process group {process_group_id} did not complete within the timeout period")
        
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
