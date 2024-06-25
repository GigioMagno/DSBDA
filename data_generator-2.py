#!/usr/bin/env python

import socket
import time
import sys
import csv
import random
import pickle
import json

calender_csv_path = '/home/ubuntu/DATASET/calendar.csv'
neighbourhoods_csv_path = '/home/ubuntu/DATASET/neighbourhoods.csv'
reviews_csv_path = '/home/ubuntu/DATASET/reviews.csv'
reviews_details_csv_path = '/home/ubuntu/DATASET/reviews_details.csv'
listings_csv_path = '/home/ubuntu/DATASET/listings.csv'
listing_details_csv_path = '/home/ubuntu/DATASET/listings_details.csv'

serialized_shuffled_dataset_path = '/home/ubuntu/DATASET/shuffled_dataset.json'

## here i collect all the paths

datasets_paths = [reviews_details_csv_path,calender_csv_path,neighbourhoods_csv_path,reviews_csv_path,listings_csv_path,listing_details_csv_path]

## this function takes in a csv file and translates it in to a python list.

def csv_to_lines(csv_path):
    lines = []
    count = 0
    with open(csv_path, 'r') as csv_file:
        line_reader = csv.reader(csv_file, delimiter = ',', quotechar = '"', quoting=csv.QUOTE_MINIMAL)  #quotechar is used to hadnle the multiple lines
        next(line_reader) # this is used to skip the headers
        for line in line_reader:
            if count%100000 == 0:
                print(f'Debug csv_to_lines {count}', end = '\r')
                sys.stdout.flush()
            lines.append(line)
            count+=1
    print('\n')
    return lines

def double_list(paths_list):
    list_of_lists = []
    count = 0
    for path in paths_list:
        list = csv_to_lines(path)
        list_of_lists.append(list)
        if count%100000 == 0:
                print(f'Debug double_list {count}', end = '\r')
                sys.stdout.flush()
        count += 1
    print('\n')
    print('Double list build')
    return list_of_lists


def touple_lottery(list_of_lists):
    count = 0
    shuffled_list = []
    for el in list_of_lists:
        for touple in el:
            shuffled_list.append(touple)
            if count%100000 == 0:
                print(f'Debug touple_lottery {count}', end = '\r')
                sys.stdout.flush()
            count += 1
    random.shuffle(shuffled_list)
    print('\n')
    print('Shuffling done')
    return shuffled_list

##     PROGRAM BODY    ##
list_of_lists = double_list(datasets_paths)
## printing the head
shuffled_dataset = touple_lottery(list_of_lists)

# I have chosen to serialize the object so every time that i stream data 
# i ease workload
print(f'Attempting to serialize shuffled_dataset at: {serialized_shuffled_dataset_path}...')
with open(serialized_shuffled_dataset_path, 'w') as json_file:
    json.dump(shuffled_dataset, json_file)
json_file.close()
print('Serialization successful!')
