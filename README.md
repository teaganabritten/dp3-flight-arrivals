# DP3: Flight Arrivals

This project tracks the arrival of flights into two major international airports: Washington Dulles and London Heathrow. I chose to compare the arrivals at these airports to look at the different airlines that serve them, and the similarities and differences in the patterns of the main airports for two of the world's most significant capital cities. 
Data is collected every fifteen minutes and is stored in DynamoDB with each flight being a document. The airport of arrival is the partition key in the table, and the time of arrival (UTC) is the sort key. Included information is the two-letter international code for the airline, flight number,and aircraft type. Additionally, the time of ingestion is stored. 
There are three API resources for this project:

- `/current` : provides the latest arrival at each airport
- `/trend` : provides information for arrivals in the last 24 hours
- `/plot` : shows a graph of arrivals by airline for the past 72 hours