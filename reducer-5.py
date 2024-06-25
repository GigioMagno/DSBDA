#!/usr/bin/python3
"""reducer.py"""
import sys
import string

last_listing_id = None
cur_name = "-"
cur_host_name = "-"
cur_neighborhood = "-"
cur_latitude = "-"
cur_longitude = "-"
cur_property_type = "-"
cur_room_type = "-"
cur_price = "-"
cur_review_scores_rating = "-"
cur_review_scores_accuracy = "-"
cur_review_scores_cleanliness = "-"
cur_review_scores_checkin = "-"
cur_review_scores_communication = "-"
cur_review_scores_location = "-"
cur_review_scores_value = "-"
cur_review_per_month = "-"
cur_comment = "-"

for line in sys.stdin:
	line = line.strip()
	# a = []
	# a = line.split("\t")
	# print(len(a))
	try:
		listing_id, name, host_name, neighborhood, latitude, longitude, property_type, room_type, price, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, review_per_month, date, comment = line.split(",")
	except Exception as e:
		print(e)
		a = line.split(",")
		print(a)
		exit()
	

	if not last_listing_id or last_listing_id != listing_id:

		last_listing_id = listing_id
		cur_name = name
		cur_host_name = host_name
		cur_neighborhood = neighborhood
		cur_latitude = latitude
		cur_longitude = longitude
		cur_property_type = property_type
		cur_room_type = room_type
		cur_price = price
		cur_review_scores_rating = review_scores_rating
		cur_review_scores_accuracy = review_scores_accuracy
		cur_review_scores_cleanliness = review_scores_cleanliness
		cur_review_scores_checkin = review_scores_checkin
		cur_review_scores_communication = review_scores_communication
		cur_review_scores_location = review_scores_location
		cur_review_scores_value = review_scores_value
		cur_review_per_month = review_per_month

	elif listing_id == last_listing_id:
		print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (listing_id, cur_name, cur_host_name, cur_neighborhood, cur_latitude,
			cur_longitude, cur_property_type, cur_room_type, cur_price, cur_review_scores_rating, cur_review_scores_accuracy, 
			cur_review_scores_cleanliness, cur_review_scores_checkin, cur_review_scores_communication,
			cur_review_scores_location, cur_review_scores_value, cur_review_per_month, date, comment))
