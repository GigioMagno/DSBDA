#!/usr/bin/env python

import socket
import time
import sys
import csv
import random
import pickle
import time
import random
import json

#serialized object path
serialized_shuffled_dataset_path = '/home/ubuntu/DATASET/shuffled_dataset.json'

#this function is responsible to write the dataset on the socket line by line so that spark
#reads it on the other end.
#the line is a touple from a randoom table

def sendData(c_socket, socket, dataset):
    print("Start sending data")
    count = 0
    
    for line in dataset:
       # print(type(line))
        try:
            # YOU MUST ADD THE RETURN CHAR OTHERWISE THE DATA DOES NOT GET FLUSHED BY THE PYTHON SOCKET
            # DATE OF DISCOVERY 00:10 31/05/2024 R-I-P
            line_string = f'{str(line)}\n'
            byte_message = line_string.encode('utf-8')
            print(f'Debug byte_message {byte_message}')
            c_socket.sendall(byte_message)
            time.sleep(random.uniform(0,0.5)) # Aggiungi un delay per non sovraccaricare il ricevitore
            count += 1
            print("Sent:", line_string) 
        except Exception as e:
            print("Error sending data:", e)
            break

try:
    # Here we try to load the serialized file that holds the shuffled dataset
    print('Loading the json list...')
    with open(serialized_shuffled_dataset_path, 'r') as json_file:
        shuffled_data = json.load(json_file)
    print('Shuffled dataset loaded successfully!')
    print('\nHead:')
    for i in range(1,10):
        print(shuffled_data[i])
    print('\n')
except:
    print('BEFORE EXECUTING BE SURE TO HAVE THE SERIALIZED OBJECT IN THE CORRECT PATH >:(')
    serialized_shuffled_dataset_path = '/home/ubuntu/DATASET/shuffled_dataset.json'
    print(f'correct path: {serialized_shuffled_dataset_path}')




###      PROGRAM BODY          ####

host = "localhost"
port = 7778
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(5)
print("Server is listening on", port)

try:
     c, addr = s.accept()
     print("Connected by", addr)
     sendData(c, s, shuffled_data)
except Exception as e:
     print("Connection error:", e)
finally:
     c.close()
