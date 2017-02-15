# Import libraries.
import os
import csv
import json
import random
import time
import datetime
import logging
import requests
from pymongo import MongoClient
from datetime import datetime

#-----------------------------------------------------------------------#
#							Function: Load Data							#
#-----------------------------------------------------------------------#
def load_data(week_number):
	# Open Log and log date.
	logger = logging.getLogger("bus_speed_crawler.data_loader")
	logger.info("Loading data for week: "+str(week_number))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0
	
	# Create datestamps for trips.
	# current_date = datetime.datetime.today().strftime('%Y/%m/%d')
	# current_date_str = datetime.datetime.strptime(current_date, '%Y/%m/%d')
	# week_date = current_date_str + datetime.timedelta(days=+1)
	# monday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	# week_date = current_date_str + datetime.timedelta(days=+2)
	# tuesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	# week_date = current_date_str + datetime.timedelta(days=+3)
	# wednesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	# week_date = current_date_str + datetime.timedelta(days=+4)
	# thursday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	# week_date = current_date_str + datetime.timedelta(days=+5)
	# friday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)

	for day in range(5):
		# Open CSV file, read headers and get length of data.
		dataFile = open('steps.csv')
		traffic_data_sheet = csv.reader(dataFile)
		headers = traffic_data_sheet.next()
		traffic_data_array = list(traffic_data_sheet)

		# Set keys to headers.
		keys = {}
		keys['trip_id'] = headers.index('trip_id')
		keys['stop_id.o'] = headers.index('stop_id.o')
		keys['stop_id.d'] = headers.index('stop_id.d')
		keys['google.transit_step_dep.time.text'] = headers.index('google.transit_step_dep.time.text')

		# Create lists for JSON Objects.
		formatted_data = []
		row_number = 0

		# Create a JSON Object for every trip in the CSV file.
		for i in range(len(traffic_data_array)):
			value = traffic_data_array[i]
			row_number = row_number+1

			# # Set datestamp for each trip.
			# if day==0:
			# 	datestamp = monday
			# if day==1:
			# 	datestamp = tuesday
			# if day==2:
			# 	datestamp = wednesday
			# if day==3:
			# 	datestamp = thursday
			# if day==4:
			# 	datestamp = friday

			time = str(value[keys['google.transit_step_dep.time.text']])
			in_time = datetime.strptime(time, "%I:%M%p")
			out_time = datetime.strftime(in_time, "%H:%M")
			split_time = str(out_time).split(":")
			hours = int(split_time[0])
			minutes = int(split_time[1])

			traffic_data_dict = {
				"unique_id": str(row_number),
				"trip_id": str(value[keys['trip_id']]),
				"from_stop_id": str(value[keys['stop_id.o']]),
				"to_stop_id": str(value[keys['stop_id.d']]),
				"weeks": week_number,
				"time_taken":"-3",
				"timestamp": {
					"hours": int(hours),
					"minutes": int(minutes),
					"day": int(day)
				}
				#"datestamp": "TBA"
			}
			
			# Append every JSON trip into the list.
			formatted_data.append(traffic_data_dict)

		# Close the CSV file.
		dataFile.close()

		# Write all JSON Objects to a JSON file. JSON file only contains all trips of the current day.
		json_items = json.dumps(formatted_data, sort_keys = True, indent = 4, separators = (',',':'))
		f = open('bus_speed_trips.json', 'w')  
		f.write(json_items)
		f.close()

		# Push JSON Objects from the file into the database.	
		page = open("bus_speed_trips.json", 'r')
		parsed = json.loads(page.read())
		for item in parsed:
			record.insert(item)
		page.close()

	# Send notification to Slack.
	url = "https://hooks.slack.com/services/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"
	data_loader_msg = "Sao Paulo 2012 Survey Bus-Speed-Crawler: Data loading succesful."
	payload={"text": data_loader_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Bus-Speed-Crawler: Error while sending data loader Slack notification.")
		logger.info(e)
		logger.info(data_loader_msg)
