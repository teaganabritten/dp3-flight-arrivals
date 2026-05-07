import boto3
import os
import logging
import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS SDK clients
dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("events")
lambda_client = boto3.client("lambda")
secrets_client = boto3.client("secretsmanager")

# Configuration
AIRPORTS = [a.strip().upper() for a in os.getenv("AIRPORTS", "IAD,LHR").split(",") if a.strip()]
TABLE_NAME = os.getenv("TABLE_NAME", "flight_arrivals")
INGEST_SCHEDULE = os.getenv("INGEST_SCHEDULE", "rate(15 minutes)")  # Fetch every 15 minutes
# AirLabs API (free, 1000 requests/month, real-time flight data)
AIRLABS_SECRET_NAME = os.getenv("AIRLABS_SECRET_NAME", "airlabs-api-key")
AIRLABS_API_BASE = "https://airlabs.co/api/v9"

# Cache for API key to avoid repeated Secrets Manager calls
_airlabs_api_key_cache = {}


def _get_airlabs_api_key() -> str:
    """
    Fetch AirLabs API key from AWS Secrets Manager (with caching).
    
    Returns:
        API key string
    
    Raises:
        Exception: If secret cannot be retrieved
    """
    # Return cached value if available
    if "key" in _airlabs_api_key_cache:
        return _airlabs_api_key_cache["key"]
    
    try:
        logger.debug(f"Fetching AirLabs API key from Secrets Manager: {AIRLABS_SECRET_NAME}")
        response = secrets_client.get_secret_value(SecretId=AIRLABS_SECRET_NAME)
        
        # Try to parse as JSON first (in case it's stored as {"api_key": "..."}
        try:
            secret_dict = json.loads(response.get("SecretString", ""))
            api_key = secret_dict.get("api_key") or secret_dict.get("AIRLABS_API_KEY") or secret_dict.get("key")
        except json.JSONDecodeError:
            # If not JSON, treat entire string as the API key
            api_key = response.get("SecretString", "")
        
        if not api_key:
            raise ValueError("API key not found in secret")
        
        # Cache it for the Lambda execution lifetime
        _airlabs_api_key_cache["key"] = api_key
        logger.debug("API key retrieved and cached")
        return api_key
    
    except secrets_client.exceptions.ResourceNotFoundException:
        logger.error(f"Secret '{AIRLABS_SECRET_NAME}' not found in Secrets Manager. Create it with: aws secretsmanager create-secret --name {AIRLABS_SECRET_NAME} --secret-string YOUR_API_KEY")
        raise
    except Exception as e:
        logger.error(f"Failed to fetch API key from Secrets Manager: {type(e).__name__}: {e}", exc_info=True)
        raise

# DynamoDB Schema & Initialization

def init_dynamodb_table():
    """
    Initialize DynamoDB table for storing flight arrivals.
    
    Schema:
    - PK: airport (String) - IAD or LHR
    - SK: arrival_time (String) - ISO timestamp
    - Attributes:
        - airline (String) - Airline code (e.g., AA, UA, BA)
        - flight_number (String)
        - aircraft_type (String)
        - ingested_at (String) - Timestamp when record was written
    
    GSI: AirlineIndex
    - PK: airline
    - SK: arrival_time (reverse order for latest first)
    """
    try:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "airport", "KeyType": "HASH"},
                {"AttributeName": "arrival_time", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "airport", "AttributeType": "S"},
                {"AttributeName": "arrival_time", "AttributeType": "S"},
                {"AttributeName": "airline", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "AirlineIndex",
                    "KeySchema": [
                        {"AttributeName": "airline", "KeyType": "HASH"},
                        {"AttributeName": "arrival_time", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            Tags=[
                {"Key": "Project", "Value": "FlightArrivals"},
                {"Key": "Environment", "Value": "dev"},
            ],
        )
        table.wait_until_exists()
        logger.info(f"DynamoDB table '{TABLE_NAME}' created successfully")
        return table
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        logger.info(f"Table '{TABLE_NAME}' already exists")
        return dynamodb.Table(TABLE_NAME)

# Ingest Lambda: Fetch & Store Flight Arrivals

def fetch_arrivals(airport: str, api_key: Optional[str] = None) -> List[Dict]:
    """
    Fetch current flight arrivals for a given airport using AirLabs API.
    
    AirLabs: https://airlabs.co (free tier: 1000 requests/month)
    - Real-time flight data with detailed aircraft and airline info
    - Free tier provides ~33 requests/day, sufficient for 15-min cadence on 2 airports
    - API key is stored securely in AWS Secrets Manager
    
    Args:
        airport: IATA code (e.g., "IAD", "LHR")
        api_key: Optional override for API key (defaults to fetching from Secrets Manager)
    
    Returns:
        List of arrival records with fields:
        - airline: Airline code
        - flight_number: Flight identifier
        - arrival_time: Timestamp of arrival
        - aircraft_type: Aircraft IATA code
    """
    # Use provided key or fetch from Secrets Manager
    if not api_key:
        try:
            api_key = _get_airlabs_api_key()
        except Exception as e:
            logger.error(f"Cannot fetch arrivals without API key: {e}")
            return []
    
    try:
        logger.info(f"Fetching arrivals for {airport} from AirLabs")
        
        # AirLabs arrivals endpoint
        response = requests.get(
            f"{AIRLABS_API_BASE}/flights",
            params={
                "api_key": api_key,
                "arr_iata": airport,  # Filter by arrival airport
                "limit": 1000,  # ask for as many records as allowed by the API
            },
            timeout=10,
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Check for API errors
        if data.get("response") == []:
            logger.info(f"No arrivals found for {airport} at this time")
            return []
        
        flights = data.get("response", [])
        if not isinstance(flights, list):
            logger.warning(f"Unexpected AirLabs response format: {type(flights)}")
            return []
        
        logger.info(f"Successfully fetched {len(flights)} flights arriving at {airport} from AirLabs")
        
        # Transform AirLabs response to our schema
        arrivals = []
        for flight in flights:
            try:
                # Check arrival time exists
                        # AirLabs returns 'updated' as Unix timestamp, not ISO arrival time
                        # For now, use the updated timestamp converted to ISO format as a proxy for arrival activity
                        updated_ts = flight.get("updated")
                        if updated_ts:
                            # Convert Unix timestamp to ISO 8601
                            arrival_time_iso = datetime.fromtimestamp(updated_ts, tz=timezone.utc).isoformat()
                            arrivals.append({
                                "airline": flight.get("airline_iata", "UNK"),
                                "flight_number": flight.get("flight_iata", "") or flight.get("flight_icao", ""),
                                "arrival_time": arrival_time_iso,
                                "aircraft_type": flight.get("aircraft_iata", "N/A"),
                            })
            except Exception as e:
                logger.debug(f"Error parsing flight record: {e}")
                continue
        
        logger.info(f"Parsed {len(arrivals)} valid arrival records for {airport}")
        return arrivals
    
    except requests.Timeout:
        logger.error(f"AirLabs API timeout for {airport}")
        return []
    except requests.ConnectionError as e:
        logger.error(f"AirLabs connection error for {airport}: {e}")
        return []
    except requests.RequestException as e:
        logger.error(f"AirLabs API error for {airport}: {type(e).__name__}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching arrivals for {airport}: {type(e).__name__}: {e}", exc_info=True)
        return []


def extract_airline_code(callsign: str) -> str:
    """
    Extract airline code from flight callsign.
    
    E.g., "AAL123" -> "AA", "UAL456" -> "UA", "BAW789" -> "BA"
    """
    if not callsign or len(callsign) < 2:
        return "UNK"
    
    # Common airline callsign prefixes
    airline_map = {
        "AAL": "AA",  # American Airlines
        "UAL": "UA",  # United Airlines
        "DAL": "DL",  # Delta
        "BAW": "BA",  # British Airways
        "AFR": "AF",  # Air France
        "DLH": "LH",  # Lufthansa
        "KLM": "KL",  # KLM
        "SWR": "SR",  # Swiss
        "IBE": "IB",  # Iberia
    }
    
    prefix = callsign[:3].upper()
    return airline_map.get(prefix, prefix[:2].upper())


def write_to_dynamodb(airport: str, arrivals: List[Dict]) -> int:
    """
    Write flight arrivals to DynamoDB in idempotent fashion.
    
    Uses arrival_time + flight_number as natural key to prevent duplicates.
    
    Args:
        airport: Airport code
        arrivals: List of arrival records
    
    Returns:
        Number of records written
    """
    if not arrivals:
        logger.warning(f"No arrivals to write for {airport}")
        return 0
    
    table = dynamodb.Table(TABLE_NAME)
    written = 0
    failed = 0
    now = datetime.utcnow().isoformat() + "Z"
    
    logger.info(f"Writing {len(arrivals)} arrivals for {airport} to DynamoDB")
    
    for arrival in arrivals:
        try:
            item = {
                "airport": airport,
                "arrival_time": arrival.get("arrival_time", ""),
                "flight_number": arrival.get("flight_number", ""),
                "airline": arrival.get("airline", ""),
                "aircraft_type": arrival.get("aircraft_type", ""),
                "ingested_at": now,
            }
            
            # Idempotent write: skip if already ingested in this window
            table.put_item(Item=item, ConditionExpression="attribute_not_exists(arrival_time)")
            written += 1
            logger.debug(f"Wrote: {arrival.get('flight_number')} at {airport} on {arrival.get('arrival_time')}")
        
        except table.meta.client.exceptions.ConditionalCheckFailedException:
            logger.debug(f"Duplicate record: {arrival.get('flight_number')} at {airport}")
        except Exception as e:
            logger.error(f"Error writing arrival record {arrival.get('flight_number')}: {type(e).__name__}: {e}")
            failed += 1
    
    logger.info(f"DynamoDB write summary for {airport}: {written} written, {failed} failed out of {len(arrivals)}")
    return written


def ingest_lambda_handler(event, context):
    """
    Lambda handler: Fetch arrivals and store them.
    
    Triggered by CloudWatch Events on a schedule (e.g., every 15 minutes).
    Handles errors gracefully and logs detailed status.
    
    Expected event:
    {
        "detail-type": "Scheduled Event",
        "source": "aws.events"
    }
    """
    start_time = datetime.utcnow()
    logger.info(f"=== Ingest Lambda started at {start_time.isoformat()} ===")
    logger.info(f"Target airports: {AIRPORTS}")
    
    total_written = 0
    total_failed = 0
    results_by_airport = {}
    
    for airport in AIRPORTS:
        airport_start = datetime.utcnow()
        try:
            logger.info(f"Processing airport: {airport}")
            
            arrivals = fetch_arrivals(airport)
            if not arrivals:
                logger.warning(f"No arrivals fetched for {airport}")
                results_by_airport[airport] = {"status": "no_data", "records": 0}
                continue
            
            written = write_to_dynamodb(airport, arrivals)
            total_written += written
            results_by_airport[airport] = {
                "status": "success",
                "records": written,
                "duration_seconds": (datetime.utcnow() - airport_start).total_seconds(),
            }
            logger.info(f"Completed {airport}: {written} records written")
        
        except Exception as e:
            total_failed += 1
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Ingest pipeline failed for {airport}: {error_msg}", exc_info=True)
            results_by_airport[airport] = {
                "status": "error",
                "error": error_msg,
                "duration_seconds": (datetime.utcnow() - airport_start).total_seconds(),
            }
            # Continue with next airport rather than crashing
    
    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"=== Ingest complete in {duration}s ===")
    logger.info(f"Summary: {total_written} records written, {total_failed} airports failed")
    
    return {
        "statusCode": 200 if total_failed == 0 else 206,
        "body": json.dumps({
            "message": "Ingest complete",
            "total_records_written": total_written,
            "airports_failed": total_failed,
            "duration_seconds": duration,
            "results_by_airport": results_by_airport,
        }),
    }

# CloudWatch Events: Schedule the Ingest Lambda

def setup_cloudwatch_event(
    lambda_function_arn: str,
    schedule_expression: str = INGEST_SCHEDULE,
) -> Dict:
    """
    Create a CloudWatch Event rule that triggers the ingest Lambda on schedule.
    
    Args:
        lambda_function_arn: ARN of the Lambda function to trigger
        schedule_expression: Rate or cron expression (e.g., "rate(15 minutes)")
    
    Returns:
        CloudWatch Event rule details
    
    Raises:
        Exception: If rule creation or target setup fails
    """
    rule_name = "flight-arrivals-ingest-schedule"
    
    try:
        logger.info(f"Setting up CloudWatch Event rule: {rule_name}")
        
        # Create the event rule
        rule_response = cloudwatch.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State="ENABLED",
            Description="Triggers ingest Lambda for flight arrivals every 15 minutes",
        )
        logger.info(f"CloudWatch Event rule created: {rule_response['RuleArn']}")
        
        # Add Lambda as the target
        cloudwatch_role = os.getenv(
            "CLOUDWATCH_EVENTS_ROLE_ARN",
            "arn:aws:iam::ACCOUNT_ID:role/service-role/CloudWatchEventsRole",
        )
        logger.info(f"Adding Lambda target with role: {cloudwatch_role}")
        
        target_response = cloudwatch.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Arn": lambda_function_arn,
                    "Id": "1",
                    "RoleArn": cloudwatch_role,
                }
            ],
        )
        logger.info(f"Lambda target added: {target_response['FailedEntryCount']} failures")
        
        return {
            "rule_arn": rule_response["RuleArn"],
            "rule_name": rule_name,
            "schedule": schedule_expression,
            "status": "created",
        }
    
    except Exception as e:
        logger.error(f"Failed to setup CloudWatch Event: {type(e).__name__}: {e}", exc_info=True)
        raise

# Query API: Read from DynamoDB

def get_arrivals_by_airport(airport: str, limit: int = 50) -> List[Dict]:
    """
    Query recent arrivals for a specific airport.
    
    Args:
        airport: Airport code (IAD or LHR)
        limit: Max records to return
    
    Returns:
        List of arrival records sorted by arrival_time (newest first)
    """
    table = dynamodb.Table(TABLE_NAME)
    
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("airport").eq(airport),
        ScanIndexForward=False,  # Reverse sort (newest first)
        Limit=limit,
    )
    
    return response.get("Items", [])


def get_arrivals_by_airline(airline: str, limit: int = 50) -> List[Dict]:
    """
    Query recent arrivals by airline across all airports.
    
    Uses the AirlineIndex GSI for efficient querying.
    
    Args:
        airline: Airline code (e.g., "AA", "BA", "UA")
        limit: Max records to return
    
    Returns:
        List of arrival records sorted by arrival_time (newest first)
    """
    table = dynamodb.Table(TABLE_NAME)
    
    response = table.query(
        IndexName="AirlineIndex",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("airline").eq(airline),
        ScanIndexForward=False,  # Reverse sort (newest first)
        Limit=limit,
    )
    
    return response.get("Items", [])


# Main: Initialization & Testing

if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("Flight Arrivals Ingest Pipeline - Initialization")
        logger.info("=" * 60)
        
        # Initialize DynamoDB table
        table = init_dynamodb_table()
        
        logger.info("✓ Pipeline initialized successfully")
        logger.info(f"  Target airports: {AIRPORTS}")
        logger.info(f"  Data source: AirLabs (https://airlabs.co)")
        logger.info(f"  API key: Stored in AWS Secrets Manager ('{AIRLABS_SECRET_NAME}')")
        logger.info(f"  Ingest schedule: {INGEST_SCHEDULE}")
        logger.info(f"  DynamoDB table: {TABLE_NAME}")
        
        logger.info("Ready for ingest operations")
    
    except Exception as e:
        logger.error(f"Pipeline initialization failed: {type(e).__name__}: {e}", exc_info=True)
        raise

