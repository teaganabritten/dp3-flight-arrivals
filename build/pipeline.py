import boto3
import numpy as np
import os
import pandas as pd
import logging
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS SDK clients
dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("events")
lambda_client = boto3.client("lambda")

# Configuration
AIRPORTS = [a.strip().upper() for a in os.getenv("AIRPORTS", "IAD,LHR").split(",") if a.strip()]
AIRPORT_ICAO = {"IAD": "KIAD", "LHR": "EGLL"}  # IATA to ICAO mapping
TABLE_NAME = os.getenv("TABLE_NAME", "flight_arrivals")
INGEST_SCHEDULE = os.getenv("INGEST_SCHEDULE", "rate(15 minutes)")  # Fetch every 15 minutes
# OpenSky Network API (free, no API key required)
OPENSKY_API_BASE = os.getenv("OPENSKY_API_BASE", "https://opensky-network.org/api")

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
    Fetch current flight arrivals for a given airport using OpenSky Network (FREE).
    
    OpenSky Network API: https://opensky-network.org/api/flights/arrival
    - No API key required (free tier has rate limits but sufficient for 15-min cadence)
    - Returns aircraft that landed in the last 24 hours
    
    Args:
        airport: IATA code (e.g., "IAD", "LHR")
        api_key: Optional username for OpenSky (increases rate limit)
    
    Returns:
        List of arrival records with fields:
        - callsign: Flight call sign
        - icao24: ICAO 24-bit address
        - arrival_time: Timestamp of arrival
        - estArrivalAirport: Arrival airport ICAO
        - airline: Extracted from callsign (approximate)
    """
    icao_code = AIRPORT_ICAO.get(airport)
    if not icao_code:
        logger.error(f"Unknown airport: {airport}")
        return []
    
    try:
        # OpenSky Network: get arrivals in last 24 hours
        response = requests.get(
            f"{OPENSKY_API_BASE}/flights/arrival",
            params={"airport": icao_code, "begin": int(datetime.utcnow().timestamp()) - 86400},
            auth=(os.getenv("OPENSKY_USERNAME"), os.getenv("OPENSKY_PASSWORD")) if os.getenv("OPENSKY_USERNAME") else None,
            timeout=10,
        )
        response.raise_for_status()
        
        flights = response.json() or []
        logger.info(f"Fetched {len(flights)} arrivals for {airport} ({icao_code})")
        
        # Transform OpenSky response to our schema
        arrivals = []
        for flight in flights:
            if flight.get("estArrivalTime") or flight.get("actualArrivalTime"):
                arrivals.append({
                    "airline": extract_airline_code(flight.get("callsign", "")),
                    "flight_number": flight.get("callsign", "").strip(),
                    "arrival_time": datetime.utcfromtimestamp(
                        flight.get("actualArrivalTime") or flight.get("estArrivalTime")
                    ).isoformat() + "Z",
                    "aircraft_type": "N/A",  # OpenSky doesn't provide aircraft type in this endpoint
                    "icao24": flight.get("icao24", ""),
                })
        
        return arrivals
    
    except requests.RequestException as e:
        logger.error(f"Failed to fetch arrivals for {airport}: {e}")
        return []  # Fail open to avoid crashing the Lambda


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
    table = dynamodb.Table(TABLE_NAME)
    written = 0
    now = datetime.utcnow().isoformat() + "Z"
    
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
        
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.debug(f"Duplicate record: {arrival.get('flight_number')} at {airport}")
        except Exception as e:
            logger.error(f"Error writing arrival record: {e}")
    
    logger.info(f"Wrote {written}/{len(arrivals)} records to {TABLE_NAME}")
    return written


def ingest_lambda_handler(event, context):
    """
    Lambda handler: Fetch arrivals and store them.
    
    Triggered by CloudWatch Events on a schedule (e.g., every 15 minutes).
    
    Expected event:
    {
        "detail-type": "Scheduled Event",
        "source": "aws.events"
    }
    """
    logger.info(f"Ingest Lambda triggered at {datetime.utcnow().isoformat()}")
    
    total_written = 0
    for airport in AIRPORTS:
        try:
            arrivals = fetch_arrivals(airport)
            if arrivals:
                written = write_to_dynamodb(airport, arrivals)
                total_written += written
        except Exception as e:
            logger.error(f"Ingest pipeline failed for {airport}: {e}")
            # Continue with next airport rather than crashing
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ingest complete",
            "records_written": total_written,
            "airports": AIRPORTS,
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
    """
    rule_name = "flight-arrivals-ingest-schedule"
    
    try:
        # Create the event rule
        rule_response = cloudwatch.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State="ENABLED",
            Description="Triggers ingest Lambda for flight arrivals every 15 minutes",
        )
        logger.info(f"CloudWatch Event rule '{rule_name}' created: {rule_response['RuleArn']}")
        
        # Add Lambda as the target
        target_response = cloudwatch.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Arn": lambda_function_arn,
                    "Id": "1",
                    "RoleArn": os.getenv(
                        "CLOUDWATCH_EVENTS_ROLE_ARN",
                        "arn:aws:iam::ACCOUNT_ID:role/service-role/CloudWatchEventsRole",
                    ),
                }
            ],
        )
        logger.info(f"Lambda target added to rule: {target_response}")
        
        return {
            "rule_arn": rule_response["RuleArn"],
            "rule_name": rule_name,
            "schedule": schedule_expression,
        }
    
    except Exception as e:
        logger.error(f"Failed to setup CloudWatch Event: {e}")
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


def get_airport_airline_summary(airport: str) -> pd.DataFrame:
    """
    Get a summary of arrivals by airline for an airport.
    
    Useful for analytics and dashboards.
    
    Args:
        airport: Airport code
    
    Returns:
        DataFrame with columns: airline, arrival_count, latest_arrival
    """
    arrivals = get_arrivals_by_airport(airport, limit=1000)
    
    if not arrivals:
        return pd.DataFrame(columns=["airline", "arrival_count", "latest_arrival"])
    
    df = pd.DataFrame(arrivals)
    summary = df.groupby("airline").agg({
        "flight_number": "count",
        "arrival_time": "max",
    }).rename(columns={
        "flight_number": "arrival_count",
        "arrival_time": "latest_arrival",
    }).reset_index()
    
    return summary.sort_values("arrival_count", ascending=False)

# Main: Initialization & Testing

if __name__ == "__main__":
    # Initialize DynamoDB table
    table = init_dynamodb_table()
    
    logger.info("Pipeline initialized successfully")
    logger.info(f"Target airports: {AIRPORTS}")
    logger.info(f"Data source: OpenSky Network (FREE - https://opensky-network.org)")
    logger.info(f"Ingest schedule: {INGEST_SCHEDULE}")
    logger.info(f"DynamoDB table: {TABLE_NAME}")
    
    # Example: Test query functions (once data is available)
    # arrivals = get_arrivals_by_airport("IAD")
    # summary = get_airport_airline_summary("IAD")
    # print(summary)

