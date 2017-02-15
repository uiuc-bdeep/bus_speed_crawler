# Import libraries.
import os
import csv
import json
import requests
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

#-----------------------------------------------------------------------#
#							Function: Make CSV 							#
#-----------------------------------------------------------------------#
def make_csv(week,day):
	# Open Log and add details.
	logger = logging.getLogger("bus_speed_crawler.csv_writer")
	logger.info("Creating CSV file for week: "+str(week)+"day "+str(day))

	# Create file name.
	MAIN_NAME = "bus-speed-crawler-"
	INCREMENTAL_FILENAME_SUFFIX = str(week)+"-"+str(day)
	NAME_EXTENSION = ".csv"
	OUTPUT_DIR = "/data/Congestion/stream/bus-speed-crawler/"
	FINAL_NAME = MAIN_NAME+INCREMENTAL_FILENAME_SUFFIX+NAME_EXTENSION

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Find all trips for the given day of the week.
	results = db.try0.find({'$and':[{"weeks":week},{"timestamp.day":day}]})

	list_of_trips = list(results)
	length = len(list_of_trips)
	print length
	logger.info("Number of trips to write to csv today: "+str(length))

	# Write JSON Objects to JSON file
	ljson = dumps(list_of_trips,sort_keys = True, indent = 4, separators = (',',':'))
	f = open('json_from_db.json', 'w')
	f.write(ljson)
	f.close()

	# Open JSON file
	file = open('json_from_db.json','r')
	json_trips = json.loads(file.read())

    # Open CSV file and write headers
	csv_file = open(FINAL_NAME, "ab+")
	z = csv.writer(csv_file)
	z.writerow(["unique_id","trip_id","from_stop_id","to_stop_id","time_taken","weeks","day"])

	# Write every trip
	for index in json_trips:
		z.writerow([index["unique_id"],index["trip_id"],index["from_stop_id"],index["to_stop_id"],index["time_taken"],index["weeks"],index["timestamp"]["day"]])
	csv_file.close()

	# Log completeion
	logger.info("Done. CSV File Created for Week: "+str(week)+" Day: "+str(day))
	print ("Done. CSV File Created for Week: "+str(week)+" Day: "+str(day))

	# Send Slack notification after successfully writing CSV file.
	url = "https://hooks.slack.com/services/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"
	csv_msg = "Sao Paulo 2012 Survey Bus-Speed-Crawler: CSV for week-"+str(week)+"-day-"+str(day)+" has been written successfully to the shared drive."
	payload1={"text": csv_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
	except requests.exceptions.RequestException as e:
		logger.info(str("Error while sending Slack Notification 2"))
		logger.info(str(e))
		logger.info(str(csv_msg))
