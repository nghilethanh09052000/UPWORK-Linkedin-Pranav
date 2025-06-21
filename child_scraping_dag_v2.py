from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import json
import logging
import sys
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our custom modules
from scripts.web_scraper import NCAAScraper
from scripts.ai_parser import AIParser
from src.v1.pipeline import run_pipeline
from src.v1.pipeline import run_pipeline_with_browser



os.environ['NO_PROXY'] = '*'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 1, 1),
}

# Define the DAG
dag = DAG(
    'nghi_child_scraping_dag_v2',
    default_args=default_args,
    description='Enhanced child DAG to scrape coach data for NCAA schools',
    schedule_interval=None,
    catchup=False,
    tags=['ncaa', 'scraping', 'child_v2'],
    max_active_tasks=32,
    max_active_runs=16
)

def save_coaches_to_s3(result, organization_id, school_name):
    """
    Save coaches data to Amazon S3 with a clear naming convention.
    
    Args:
        result: The pipeline result containing extracted data
        organization_id: The organization/school ID
        school_name: The name of the school
        
    Returns:
        The S3 key (path) to the saved JSON file
    """
    import json
    from datetime import datetime
    
    # Create a timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get the S3 bucket from Airflow Variables
    s3_bucket = Variable.get('coaches_data_bucket', 'your-default-bucket-name')
    
    # Define the S3 key (path)
    base_path = "coaches_json"
    s3_key = f"{base_path}/coaches_{organization_id}_{timestamp}_raw_coaches.json"
    latest_key = f"{base_path}/coaches_{organization_id}_latest.json"
    
    # Prepare the data to save
    coaches_data = result.get("extracted_data", [])
    
    # If it's a CoachingData object, convert it to a list of dictionaries
    if hasattr(coaches_data, "coaches"):
        coaches_list = coaches_data.coaches
        serializable_coaches = []
        
        for coach in coaches_list:
            coach_dict = {
                "name": coach.name,
                "title": coach.title,
                "email": coach.email,
                "phone": coach.phone,
                "fullBioLink": coach.fullBioLink,
                "sport": coach.sport,
                "orgId": coach.orgId or organization_id
            }
            serializable_coaches.append(coach_dict)
        
        coaches_data = serializable_coaches
    
    # Create the full data structure with metadata
    data_to_save = {
        "metadata": {
            "school_name": school_name,
            "organization_id": organization_id,
            "timestamp": timestamp,
            "source_url": result.get("url", ""),
            "scrape_date": datetime.now().isoformat()
        },
        "coaches": coaches_data,
        "emails": result.get("emails", [])
    }
    
    # Convert to JSON string
    json_data = json.dumps(data_to_save, indent=2)
    
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    
    # Upload to S3
    s3_hook.load_string(
        string_data=json_data,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    # Also save a "latest" version for this organization
    s3_hook.load_string(
        string_data=json_data,
        key=latest_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    # Return the S3 URL
    s3_url = f"s3://{s3_bucket}/{s3_key}"
    return s3_url

def process_coaches_with_browser_pipeline(**context):
    """
    Process coaches data using the enhanced browser-based pipeline.
    """
    # Get the task instance
    ti = context['ti']
    
    # Get the school data and roster URL
    school_data = ti.xcom_pull(task_ids='process_school', key='school_data')
    roster_url = ti.xcom_pull(task_ids='process_school', key='roster_url')
    
    school_name = school_data.get('school_name', 'Unknown School')
    organization_id = school_data.get('ncaa_institution_id', '')
    
    if not roster_url or roster_url == "Not found":
        error_msg = f"Could not find valid roster URL for {school_name}. Cannot run browser pipeline."
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    if not organization_id:
        error_msg = f"No organization ID found for {school_name}. Cannot run pipeline."
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    logger.info(f"Running browser pipeline for {school_name} with URL: {roster_url}")
    
    # Run the enhanced pipeline
    try:
        result = run_pipeline_with_browser(roster_url, organization_id)
        print(f"Browser pipeline result: {result}")
        
        if not result or not result.get("success"):
            error_msg = f"Browser pipeline failed for {school_name}: {result.get('error', 'Unknown error')}"
            logger.error(error_msg)
            raise AirflowFailException(error_msg)
        
        # Convert CoachingData to a serializable format if needed
        if "extracted_data" in result and hasattr(result["extracted_data"], "coaches"):
            coaches_list = result["extracted_data"].coaches
            serializable_coaches = []
            for coach in coaches_list:
                coach_dict = {
                    "name": coach.name,
                    "title": coach.title,
                    "email": coach.email,
                    "phone": coach.phone,
                    "fullBioLink": coach.fullBioLink,
                    "sport": coach.sport,
                    "orgId": coach.orgId or organization_id
                }
                serializable_coaches.append(coach_dict)
            result["extracted_data"] = serializable_coaches
        
        # Log success information
        logger.info(f"Browser pipeline completed successfully for {school_name}")
        
        # Get the number of coaches safely
        coach_count = len(result["extracted_data"]) if isinstance(result["extracted_data"], list) else (
            len(result["extracted_data"].coaches) if hasattr(result["extracted_data"], "coaches") else "unknown number of"
        )
        
        logger.info(f"Extracted {coach_count} coaches")
        logger.info(f"Found {len(result['emails'])} emails")
        
        # Save the data to S3
        s3_url = save_coaches_to_s3(result, organization_id, school_name)
        logger.info(f"Saved coaches data to S3: {s3_url}")
        
        # Push the result to XCom for downstream tasks
        ti.xcom_push(key='browser_pipeline_result', value=result)
        ti.xcom_push(key='coaches_s3_url', value=s3_url)
        
        return result
    except Exception as e:
        error_msg = f"Browser pipeline failed for {school_name}: {str(e)}"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)

def load_schools_data():
    """Load schools data from the CSV file."""
    import csv
    csv_path = os.path.join(os.environ.get('AIRFLOW_HOME', '.'), 'data', 'prepopulated_urls.csv')
    try:
        schools = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)  # Using DictReader to read the CSV as a list of dictionaries
            for row in reader:
                schools.append(row)  # Add each row as a dictionary to the schools list
        
        logger.info(f"Loaded {len(schools)} schools from {csv_path}")
        return schools
    except Exception as e:
        logger.error(f"Error loading schools data: {str(e)}")
        raise

def process_school(**context):
    """Process a single school's data."""
    import csv
    from datetime import datetime
    
    school_data = context['school_data']
    school_name = school_data.get('school_name', 'Unknown School')
    school_id = school_data.get('ncaa_institution_id', 'Unknown ID')
    
    logger.info(f"Processing school: {school_name} (ID: {school_id})")
    
    # Validate required fields
    required_fields = ['ncaa_institution_id', 'school_name', 'athletics_url']
    missing_fields = [field for field in required_fields if not school_data.get(field)]
    
    if missing_fields:
        error_msg = f"Missing required fields for {school_name}: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Ensure athletics URL has proper format
    athletics_url = school_data['athletics_url']
    if athletics_url.startswith('//'):
        athletics_url = athletics_url.replace('//', '')
    if not athletics_url.startswith(('http://', 'https://')):
        athletics_url = f"https://{athletics_url}"
    school_data['athletics_url'] = athletics_url
    
    # Find football page
    scraper = NCAAScraper()
    football_url = scraper.find_football_link(athletics_url)
    
    if football_url == "Not found":
        error_msg = f"Could not find football program page for {school_name}"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    logger.info(f"Found football program URL for {school_name}: {football_url}")
    
    # Find roster/coaches page
    roster_url = scraper.find_coaches_or_roster_link(football_url)
    
    if roster_url == "Not found":
        error_msg = f"Could not find coaches page for {school_name}"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    
    logger.info(f"Found coaches URL for {school_name}: {roster_url}")
    
    # Prepare result data
    result = {
        'school_name': school_name,
        'school_id': school_id,
        'football_url': football_url,
        'roster_url': roster_url
    }
    
    # Export to CSV
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '.'), 'data/output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Use a single CSV file for all schools
        csv_path = os.path.join(output_dir, 'schools_data.csv')
        
        # Define CSV fields
        fields = [
            'school_id', 'school_name', 'athletics_url', 'football_url', 
            'roster_url', 'conference', 'division', 'timestamp'
        ]
        
        # Prepare data for CSV
        csv_data = {
            'school_id': school_id,
            'school_name': school_name,
            'athletics_url': athletics_url,
            'football_url': football_url,
            'roster_url': roster_url,
            'conference': school_data.get('conference', ''),
            'division': school_data.get('division', ''),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Write to CSV (append mode)
        file_exists = os.path.isfile(csv_path)
        with open(csv_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            if not file_exists:
                writer.writeheader()
            writer.writerow(csv_data)
        
        logger.info(f"Successfully appended data to CSV: {csv_path}")
        
    except Exception as e:
        logger.error(f"Error exporting to CSV: {str(e)}")
        # Don't raise the exception, just log it and continue
    
    return result

# Load schools data
schools = load_schools_data()

# Create dynamic tasks for each school
for school in schools:
    school_id = str(school.get('orgId', ''))
    school_name = school.get('school_name', '')
    
    if not school_id or not school_name:
        logger.warning(f"Skipping school with missing ID or name: {school}")
        continue
    
    # Prepare school data
    school_data = {
        "athletics_url": school.get('athletic_web_url', ''),
        "conference": school.get('conference', ''),
        "division": school.get('division', ''),
        "ncaa_institution_id": school_id,
        "school_name": school_name,
        "source": "api_trigger",
        "football_program_url": school.get('football_program_url', ''),
        "roster_url": school.get('roster_url', ''),
        "coaching_staff_url": school.get('coaching_staff_url', '')
    }
    
    # Create task for this school
    task = PythonOperator(
        task_id=f'process_school_{school_id}',
        python_callable=process_school,
        op_kwargs={'school_data': school_data},
        provide_context=True,
        dag=dag,
    )
    
    # Create browser pipeline task for this school
    # browser_pipeline_task = PythonOperator(
    #     task_id=f'process_coaches_browser_{school_id}',
    #     python_callable=process_coaches_with_browser_pipeline,
    #     provide_context=True,
    #     dag=dag,
    # )
    
    # Set task dependencies
    #task >> browser_pipeline_task 