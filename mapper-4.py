#!/usr/bin/python3
"""mapper.py"""
import sys
import re

for line in sys.stdin:
	line = line.strip()
	splits = line.split(",")
	
	listing_id = "-"
	name = "-"
	host_name = "-"
	neighborhood = "-"
	latitude = "-"
	longitude = "-"
	property_type = "-"
	room_type = "-"
	price = "-"
	review_scores_rating = "-"
	review_scores_accuracy = "-"
	review_scores_cleanliness = "-"
	review_scores_checkin = "-"
	review_scores_communication = "-"
	review_scores_location = "-"
	review_scores_value = "-"
	review_per_month = "-"
	date = "-"
	comment = "-"

	#reviews_details
	if len(splits) == 3:
		listing_id = splits[0]
		date = splits[1]
		comment = splits[2]

		#listing details
	elif len(splits) == 17:
		listing_id = splits[0]
		name = splits[1]
		host_name = splits[2]
		neighborhood = splits[3]
		latitude = splits[4]
		longitude = splits[5]
		property_type = splits[6]
		room_type = splits[7]
		price = splits[8]
		review_scores_rating = splits[9]
		review_scores_accuracy = splits[10]
		review_scores_cleanliness = splits[11]
		review_scores_checkin = splits[12]
		review_scores_communication = splits[13]
		review_scores_location = splits[14]
		review_scores_value = splits[15]
		review_per_month = splits[16]

	else:
		continue

	print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (listing_id, name, host_name, neighborhood, 
		latitude, longitude, property_type, room_type, price, review_scores_rating, review_scores_accuracy, 
		review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, 
		review_scores_value, review_per_month, date, comment))

#The sort command available in shell implements shell sort -> quickly for big amount of data