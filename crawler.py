# Import libraries.
import os
import json
import requests
import datetime
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId

#-----------------------------------------------------------------------#
#							Function: Crawl Trip						#
#-----------------------------------------------------------------------#
def crawl_trip(jsonObj):
	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Open Log. Parse and log trip ID.
	logger = logging.getLogger("bus_speed_crawler.crawler")
	db_id = jsonObj['_id']
	unique_id = jsonObj['unique_id']
	trip_id = jsonObj['trip_id']
	stop_or = jsonObj['from_stop_id']
	stop_de = jsonObj['to_stop_id']

	# Log trip being crawled.
	logger.info("Crawling Trip: "+str(unique_id))


	# Create URL.
	final_url = "http://api.scipopulis.com/v1/trip/"+str(trip_id)+"/prediction/at_stop/"+str(stop_or)

	# Query API.
	try:
		r = requests.get(final_url)
		logger.info("Queried API for trip: "+str(unique_id))

		# Print to terminal, so can check response in docker logs.
		print r.content

		# Convert response to JSON.
		response = json.loads(r.content)

		# Set primary response to -3.
		final_result = "-3"
		dest_eta = -3
		orig_eta = -3
		if 'stops' in response:
			for entry in response["stops"]:
				if int(entry["stop_id"]) == int(stop_or):
					orig_eta = entry["eta"]
				if int(entry["stop_id"]) == int(stop_de):
					dest_eta = entry["eta"]
			if dest_eta != -3 and orig_eta != -3:
				if isinstance(dest_eta,(int,long)) and isinstance(orig_eta,(int,long)):
					result = dest_eta - orig_eta
					final_result = str(result)
		
	# If an exception is generated.
	except requests.exceptions.RequestException as e:
		logger.info("Error while crawling trip: "+str(unique_id))
		logger.info(str(e))
		final_result = "-1"

	# Update database
	db.try0.update({"_id" : db_id}, {"$set": {"time_taken": final_result}})
	logger.info("Modified Database for trip: "+str(unique_id))
