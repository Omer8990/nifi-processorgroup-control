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
        connection_id = connection_id or "nifi_default"
        try:
            connection_info = Variable.get(connection_id, deserialize_json=True)
            self.base_url = connection_info.get("base_url", "http://nifi-standalone:8080/nifi-api/")
            self.auth_type = connection_info.get("auth_type", "none")  # none, token, basic
            self.token = connection_info.get("token")
            self.username = connection_info.get("username")
            self.password = connection_info.get("password")
        except (KeyError, json.JSONDecodeError, TypeError): # Added TypeError for Variable.get returning None if not found and no default
            logger.warning(f"Could not find or parse Airflow Variable '{connection_id}', or it was not a valid JSON. Using default NiFi settings.")
            self.base_url = "http://nifi-standalone:8080/nifi-api/"
            self.auth_type = "none"
            self.token = None
            self.username = None
            self.password = None
        except Exception as e: # Catch other potential Variable.get issues like 'Variable <name> does not exist'
             logger.warning(f"Error fetching Airflow Variable '{connection_id}': {e}. Using default NiFi settings.")
             self.base_url = "http://nifi-standalone:8080/nifi-api/"
             self.auth_type = "none"
             self.token = None
             self.username = None
             self.password = None


        if not self.base_url.endswith('/'):
            self.base_url += '/'

        self.headers = {'Content-Type': 'application/json'}
        if self.auth_type == "token" and self.token:
            self.headers['Authorization'] = f'Bearer {self.token}'

        logger.info(f"Initializing NiFi API client with base URL: {self.base_url} and auth type: {self.auth_type}")

    def _make_request(self, method: str, endpoint: str, data=None, params=None):
        """Make HTTP request to NiFi API with error handling"""
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
        url = self.base_url + endpoint
        logger.debug(f"Making {method} request to: {url} with data: {data} and params: {params}")

        auth = None
        if self.auth_type == "basic" and self.username and self.password:
            auth = (self.username, self.password)

        try:
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params, auth=auth, timeout=30)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data, auth=auth, timeout=30)
            elif method == 'PUT':
                response = requests.put(url, headers=self.headers, json=data, auth=auth, timeout=30)
            elif method == 'DELETE':
                response = requests.delete(url, headers=self.headers, auth=auth, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            if not response.content: # Handle empty response (e.g., 204 No Content)
                return None
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request to {url} failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text[:500]}...")
                if e.response.status_code == 404:
                    return None # For 404 errors, return None instead of raising immediately
            raise

    def start_process_group(self, process_group_id: str):
        """Start all components in a process group"""
        try:
            data = {
                "id": process_group_id,
                "state": "RUNNING"
            }

            response = self._make_request('PUT', f'flow/process-groups/{process_group_id}', data=data)
            logger.info(f"Attempted to start components in process group {process_group_id}. Response: {response}")
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
            response = self._make_request('PUT', f'flow/process-groups/{process_group_id}', data=data)
            logger.info(f"Attempted to stop components in process group {process_group_id}. Response: {response}")
            return response
        except Exception as e:
            logger.error(f"Error stopping process group {process_group_id}: {e}")
            return None

    def get_process_group_status(self, process_group_id: str):
        """Get detailed status of a process group"""
        try:
            status_response = self._make_request('GET', f'flow/process-groups/{process_group_id}/status')
            if status_response and 'processGroupStatus' in status_response and 'aggregateSnapshot' in status_response['processGroupStatus']:
                aggregated_status = status_response['processGroupStatus']['aggregateSnapshot']
                return {
                    'active_threads': aggregated_status.get('activeThreadCount', 0),
                    'queued_count': aggregated_status.get('queuedCount', '0').replace(',', ''), # Ensure queuedCount is integer
                    'queued_size_bytes': aggregated_status.get('queuedContentSize', '0').replace(',', '').split(' ')[0], # Extract bytes
                    'input_count': aggregated_status.get('flowFilesReceived', '0').replace(',', ''), # flowFilesReceived is better for "input"
                    'output_count': aggregated_status.get('flowFilesSent', '0').replace(',', ''), # flowFilesSent for "output"
                    'bytes_read': aggregated_status.get('bytesRead', 0),
                    'bytes_written': aggregated_status.get('bytesWritten', 0),
                    'errors': len(aggregated_status.get('activeRemotePortCount',[])) + len(aggregated_status.get('inactiveRemotePortCount',[])) # This 'errors' field was a bit ambiguous
                }
            else:
                logger.warning(f"Could not retrieve valid status snapshot for PG {process_group_id}. Response: {status_response}")
                return None
        except Exception as e:
            logger.error(f"Error getting process group status for {process_group_id}: {e}")
            return None


    def _log_final_stats(self, process_group_id: str, status: dict, outcome: str):
        """Helper to log final statistics."""
        log_level = logging.INFO if outcome == "COMPLETED" else logging.WARNING
        if status:
            logger.log(log_level,
                f"PG {process_group_id} Final Status ({outcome}): "
                f"Input: {status.get('input_count', 'N/A')}, "
                f"Output: {status.get('output_count', 'N/A')}, "
                f"Queued: {status.get('queued_count', 'N/A')} ({status.get('queued_size_bytes', 'N/A')} bytes), "
                f"Errors (indicator): {status.get('error_indicator', 'N/A')}, " # Using the new 'error_indicator' key
                f"Active Threads: {status.get('active_threads', 'N/A')}"
            )
        else:
            logger.log(log_level, f"PG {process_group_id} Final Status ({outcome}): Status unavailable.")


    def wait_for_process_group_completion(self, process_group_id: str, check_interval: int = 10, timeout: int = 300):
        """
        Waits for a NiFi process group to complete.
        Completion means: no active threads, no queued data, and no indicated errors.
        Includes a stability check for quiescent flows.
        """
        start_time = time.time()
        # Store (input_count, output_count, queued_count) for change detection
        prev_status_metrics_tuple = None
        unchanged_stable_checks = 0
        consecutive_checks_for_stability = 3 # Needs 3 stable checks

        logger.info(f"Waiting for process group {process_group_id} to complete (timeout: {timeout}s, interval: {check_interval}s)...")

        while time.time() - start_time < timeout:
            current_status_dict = self.get_process_group_status(process_group_id)

            if current_status_dict is None:
                logger.warning(f"Could not get status for PG {process_group_id}. Retrying in {check_interval}s.")
                time.sleep(check_interval)
                prev_status_metrics_tuple = None # Reset as we missed a datapoint
                unchanged_stable_checks = 0
                continue

            active_threads = current_status_dict.get('active_threads', -1) # Use .get for safety
            queued_count = current_status_dict.get('queued_count', -1)
            error_indicator = current_status_dict.get('error_indicator', 0) # Default to 0 if not present
            input_count = current_status_dict.get('input_count', -1)
            output_count = current_status_dict.get('output_count', -1)
            queued_size_bytes = current_status_dict.get('queued_size_bytes', -1)


            logger.info(
                f"PG {process_group_id} Status: Active Threads: {active_threads}, "
                f"Queued: {queued_count} ({queued_size_bytes} bytes), Errors (indicator): {error_indicator}, "
                f"Input: {input_count}, Output: {output_count}"
            )

            # Primary success condition: stopped, empty, and no errors
            if active_threads == 0 and queued_count == 0 and error_indicator == 0:
                logger.info(f"Process group {process_group_id} completed successfully: 0 active threads, 0 queued, 0 errors.")
                self._log_final_stats(process_group_id, current_status_dict, "COMPLETED")
                return True

            # Stability check:
            # If flow is stopped (0 active threads) and has no indicated errors,
            # check if key metrics (input, output, queued) are stable.
            if active_threads == 0 and error_indicator == 0:
                current_metrics_tuple = (input_count, output_count, queued_count)
                if prev_status_metrics_tuple is not None and current_metrics_tuple == prev_status_metrics_tuple:
                    unchanged_stable_checks += 1
                    logger.info(f"PG {process_group_id}: Activity stopped, no errors. Status unchanged for {unchanged_stable_checks} check(s). Queued: {queued_count}.")

                    if unchanged_stable_checks >= consecutive_checks_for_stability:
                        # If it's stable for N checks, AND queues are now empty, it's a success.
                        if queued_count == 0:
                            logger.info(
                                f"Process group {process_group_id} deemed complete after {unchanged_stable_checks} "
                                f"stable checks with 0 active threads, 0 errors, and 0 queued items."
                            )
                            self._log_final_stats(process_group_id, current_status_dict, "COMPLETED")
                            return True
                        else:
                            # Stable, 0 active threads, 0 errors, BUT QUEUES NOT EMPTY.
                            # This is a sign of a stuck flow. Continue monitoring; will eventually timeout.
                            logger.warning(
                                f"PG {process_group_id}: Activity stopped, no errors, status stable for {unchanged_stable_checks} checks, "
                                f"BUT QUEUES NOT EMPTY ({queued_count}). Flow might be stuck or waiting for external conditions."
                            )
                            # We let it continue to timeout rather than exiting false immediately,
                            # as some flows might have intentional pauses or very slow drains.
                else:
                    # Status changed (e.g., queues decreased) or first check where active_threads == 0.
                    unchanged_stable_checks = 1 # Start/reset to 1 because this is the first of potentially stable checks
                    if prev_status_metrics_tuple is not None : # Log only if it's a change, not the very first check
                        logger.info(f"PG {process_group_id}: Activity stopped, no errors. Status changed or first stable observation, resetting stability counter to 1. Queued: {queued_count}.")
                prev_status_metrics_tuple = current_metrics_tuple
            else: # Active threads > 0 or there are indicated errors
                unchanged_stable_checks = 0 # Reset stability counter
                prev_status_metrics_tuple = None # Reset comparison baseline
                if error_indicator > 0:
                    logger.warning(f"PG {process_group_id} has an error indicator value of {error_indicator}. Cannot complete successfully while errors are indicated.")
                    # Depending on requirements, you might want to return False immediately if errors are critical.
                    # For now, it continues to poll until timeout or errors clear.

            time.sleep(check_interval)

        logger.warning(f"Timeout reached while waiting for process group {process_group_id} to complete.")
        # Log final status at timeout, if available from the last check
        self._log_final_stats(process_group_id, current_status_dict if 'current_status_dict' in locals() else None, "TIMEOUT")
        return False


# --- Airflow DAG and Tasks ---

def run_nifi_process_group(**kwargs):
    """
    Run an existing NiFi process group and wait for completion.
    Returns: True if successful, False otherwise.
    """
    # Get the process group ID from Airflow Variables
    # Ensure 'your-process-group-id' is replaced with a sensible default or is always set in Airflow UI
    process_group_id = Variable.get("existing_process_group_id", default_var="your-process-group-id")
    if process_group_id == "your-process-group-id":
        logger.warning("Using placeholder 'your-process-group-id'. Please set the 'existing_process_group_id' Airflow Variable.")
        # Depending on policy, you might want to raise an error here if not set.
        # For this example, we'll proceed but it will likely fail if the ID is not valid.

    # Get NiFi connection ID from Airflow Variables or use default
    nifi_connection_id = Variable.get("nifi_connection_id", default_var="nifi_default")

    logger.info(f"Attempting to run NiFi process group: {process_group_id} using connection: {nifi_connection_id}")

    nifi_client = NiFiAPI(connection_id=nifi_connection_id)
    success = False

    try:
        start_response = nifi_client.start_process_group(process_group_id)
        # Basic check if start command was accepted (NiFi API usually returns component entity for successful PUT)
        if start_response is None and nifi_client.auth_type != "none": # if auth is enabled, None response likely means failure
             logger.error(f"Failed to send start command to process group {process_group_id}. Check NiFi connectivity and permissions.")
             # No XCom push here as the task itself failed before waiting
             raise Exception(f"NiFi start command failed for {process_group_id}")


        logger.info(f"Waiting for process group {process_group_id} to complete...")
        success = nifi_client.wait_for_process_group_completion(process_group_id)

        if success:
            logger.info(f"Process group {process_group_id} completed successfully.")
        else:
            logger.warning(f"Process group {process_group_id} did not complete successfully (timed out or ended with issues).")

    except Exception as e:
        logger.error(f"Error during NiFi process group execution for {process_group_id}: {e}", exc_info=True)
        success = False # Ensure success is false on any exception
        # Attempt to stop the process group as a cleanup step, regardless of wait outcome
        logger.info(f"Attempting to stop process group {process_group_id} due to error or incomplete run.")
        try:
            nifi_client.stop_process_group(process_group_id)
        except Exception as cleanup_error:
            logger.error(f"Error stopping process group {process_group_id} during cleanup: {cleanup_error}", exc_info=True)
        # Re-raise the original exception if you want the Airflow task to fail hard
        # If you just want to record failure via XCom and proceed, don't re-raise or handle differently.
        # For now, let it fail the task.
        raise
    finally:
        # Ensure XCom is pushed in most scenarios reflecting the outcome of the NiFi job part.
        # If an exception occurs before `success` is determined by `wait_for_process_group_completion`,
        # `success` will retain its initial `False` value.
        if 'ti' in kwargs:
             kwargs['ti'].xcom_push(key='nifi_run_success', value=success)
        else:
            logger.warning("Task instance ('ti') not found in kwargs. Cannot push XCom value for 'nifi_run_success'.")


    if not success:
        # If not successful, raise an exception to make the Airflow task fail
        raise Exception(f"NiFi process group {process_group_id} failed to complete successfully.")

    return success # Though if it's not success, an exception is raised above.

def run_spark_job(**kwargs):
    """
    Simulates running a Spark job.
    Returns: True if successful.
    """
    logger.info("Starting Spark job simulation...")
    # In a real scenario, this would submit a Spark job and monitor it.
    # For simulation, we'll just wait a bit.
    processing_time = 10 # Simulate 10 seconds of Spark processing
    time.sleep(processing_time)
    logger.info(f"Spark job simulation completed successfully after {processing_time} seconds.")
    return True


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Set to 0 for easier debugging of NiFi interaction logic; increase for production
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nifi_spark_orchestration_v2', # Renamed for clarity
    default_args=default_args,
    description='Orchestrates a NiFi process group followed by a PySpark job (v2 with enhanced NiFi monitoring)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1), # Adjust start_date if needed
    catchup=False,
    tags=['nifi', 'pyspark', 'orchestration'],
) as dag:

    nifi_task = PythonOperator(
        task_id='run_nifi_flow_and_wait',
        python_callable=run_nifi_process_group,
        # provide_context=True is default in Airflow 2.0+ for PythonOperator
    )

    pyspark_task = PythonOperator(
        task_id='run_simulated_spark_job',
        python_callable=run_spark_job,
    )

    # Set task dependencies
    nifi_task >> pyspark_task


#  is_process_group_completed_successfully defines success as:

# No active threads (status['active_threads'] == 0)
# No errors (status['errors'] == 0)
# AND (data was processed (status['input_count'] > 0 or status['output_count'] > 0) OR status was unchanged from a previous check).
