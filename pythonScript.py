####
# Python Script to Gather Data from the Modo API
####

import requests
import json
import numpy as np
import pandas as pd
import datetime
import logging
import time
import sys

# Create log file
currentDate = datetime.date.today()
logFileName = '%s_modo_logging_file.log' % currentDate
logging.basicConfig(filename=logFileName,
                    level=logging.INFO,
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    format='%(asctime)s %(levelname)s %(message)s')

logging.info('Starting script')

# Create dataframe to fill later
emptyDF = pd.DataFrame(
    {'date': [],
     'carId': [],
     'city': [],
     'neighbourhood': [],
     'region': [],
     'lat': [],
     'lng': []
    })

# Stamp the name of the CSV with the start date
fileName = 'output_%s.csv' % currentDate
logging.info('File to written is named %s' % fileName)

# Write empty df to disk to append later
logging.info('Writing empty csv to disk')
emptyDF.to_csv(fileName)

# Start loop to collect data
# Loop through the funciton every 15 minutes, and run for 10 days
numberOfDays = 10
hourSplit = 4
numberOfIterations = hourSplit * 24 * numberOfDays
currentIteration = 0
maxTries = 3
apiUrl = 'https://bookit.modo.coop/api/v2/location_list'


while currentIteration <= numberOfIterations:
    #Set current iteration
    logging.info('Starting iteration cycle: %d' % currentIteration)    
    
    # Request data
    logging.info('Attemping to fetch data')
    currentTries = 0
    r2 = None
    
    while True:
        r2 = requests.get(apiUrl)
        logging.info(r2.status_code)
        
        if r2.status_code == 200:
            break
        
        if r2.status_code == 500 and currentTries < maxTries:
            tries += 1
            logging.info("Data fetch failed. Waiting 10 seconds.")
            time.sleep(10) 
        continue
        
        if currentTries == maxTries:
            logging.info("Maximim tries reached. Exiting program.")
            sys.exit()
        
    
    logging.info("Data fetch successful. Continuing.")
    res2 = r2.json()

    # Encode data
    data2 = json.dumps(res2, ensure_ascii=False).encode('utf-8')
    data2 = json.loads(data2)
    
    # Set temp variables
    lengthOfCars = len(data2['Response']['Locations'])
    keyToIterate = data2['Response']['Locations'].keys()
    id_values = []
    date_values = []
    city_values = []
    neighbourhood_values = []
    region_values = []
    lat_values = []
    long_values = []

    # Create array of current datetime
    currentDateTime = datetime.datetime.now().isoformat()
    logging.info('Current iteration is happening at %s' % currentDateTime)
    dateArray = np.array([currentDateTime for i in xrange(lengthOfCars)])

    for record in keyToIterate:
        id_val = data2['Response']['Locations'][record]['ID']
        name_val = data2['Response']['Locations'][record]['Name']
        city_val = data2['Response']['Locations'][record]['City']
        neighbourhood_val = data2['Response']['Locations'][record]['Neighbourhood']
        region_val = data2['Response']['Locations'][record]['Region']
        lat_val = data2['Response']['Locations'][record]['Latitude']
        lng_val = data2['Response']['Locations'][record]['Longitude']

        id_values.append(id_val)
        city_values.append(city_val)
        neighbourhood_values.append(neighbourhood_val)
        region_values.append(region_val)
        lat_values.append(lat_val)
        long_values.append(lng_val)
    
    logging.info('Creating dataframe to append')
    tempDF = pd.DataFrame(
        {'date': dateArray,
         'carId': id_values,
         'city': city_values,
         'neighbourhood': neighbourhood_values,
         'region': region_values,
         'lat': lat_values,
         'lng': lng_values
        })

    # Append data to disk
    logging.info('Appending to existing file')
    tempDF.to_csv(fileName, header=None, mode="a")

    currentIteration += 1
    logging.info('Sleeping until next iteration')
    time.sleep((60 / hourSplit) * 60) 
    logging.info('Done sleeping')
        
logging.info('Completed %d iterations' % currentIteration)
####
# Python Script to Gather Data from the Modo API
####

import requests
import json
import numpy as np
import pandas as pd
import datetime
import logging
import time
import sys

# Create log file
currentDate = datetime.date.today()
logFileName = '%s_modo_logging_file.log' % currentDate
logging.basicConfig(filename=logFileName,
                    level=logging.INFO,
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    format='%(asctime)s %(levelname)s %(message)s')

logging.info('Starting script')

# Create dataframe to fill later
emptyDF = pd.DataFrame(
    {'date': [],
     'carId': [],
     'city': [],
     'neighbourhood': [],
     'region': [],
     'lat': [],
     'lng': []
    })

# Stamp the name of the CSV with the start date
fileName = 'output_%s.csv' % currentDate
logging.info('File to written is named %s' % fileName)

# Write empty df to disk to append later
logging.info('Writing empty csv to disk')
emptyDF.to_csv(fileName)

# Start loop to collect data
# Loop through the funciton every 15 minutes, and run for 10 days
numberOfDays = 10
hourSplit = 4
numberOfIterations = hourSplit * 24 * numberOfDays
currentIteration = 0
maxTries = 3
apiUrl = 'https://bookit.modo.coop/api/v2/location_list'


while currentIteration <= numberOfIterations:
    #Set current iteration
    logging.info('Starting iteration cycle: %d' % currentIteration)    
    
    # Request data
    logging.info('Attemping to fetch data')
    currentTries = 0
    r2 = None
    
    while True:
        r2 = requests.get(apiUrl)
        logging.info(r2.status_code)
        
        if r2.status_code == 200:
            break
        
        if r2.status_code == 500 and currentTries < maxTries:
            tries += 1
            logging.info("Data fetch failed. Waiting 10 seconds.")
            time.sleep(10) 
        continue
        
        if currentTries == maxTries:
            logging.info("Maximim tries reached. Exiting program.")
            sys.exit()
        
    
    logging.info("Data fetch successful. Continuing.")
    res2 = r2.json()

    # Encode data
    data2 = json.dumps(res2, ensure_ascii=False).encode('utf-8')
    data2 = json.loads(data2)
    
    # Set temp variables
    lengthOfCars = len(data2['Response']['Locations'])
    keyToIterate = data2['Response']['Locations'].keys()
    id_values = []
    date_values = []
    city_values = []
    neighbourhood_values = []
    region_values = []
    lat_values = []
    long_values = []

    # Create array of current datetime
    currentDateTime = datetime.datetime.now().isoformat()
    logging.info('Current iteration is happening at %s' % currentDateTime)
    dateArray = np.array([currentDateTime for i in xrange(lengthOfCars)])

    for record in keyToIterate:
        id_val = data2['Response']['Locations'][record]['ID']
        name_val = data2['Response']['Locations'][record]['Name']
        city_val = data2['Response']['Locations'][record]['City']
        neighbourhood_val = data2['Response']['Locations'][record]['Neighbourhood']
        region_val = data2['Response']['Locations'][record]['Region']
        lat_val = data2['Response']['Locations'][record]['Latitude']
        lng_val = data2['Response']['Locations'][record]['Longitude']

        id_values.append(id_val)
        city_values.append(city_val)
        neighbourhood_values.append(neighbourhood_val)
        region_values.append(region_val)
        lat_values.append(lat_val)
        long_values.append(lng_val)
    
    logging.info('Creating dataframe to append')
    tempDF = pd.DataFrame(
        {'date': dateArray,
         'carId': id_values,
         'city': city_values,
         'neighbourhood': neighbourhood_values,
         'region': region_values,
         'lat': lat_values,
         'lng': long_values
        })

    # Append data to disk
    logging.info('Appending to existing file')
    tempDF.to_csv(fileName, header=None, mode="a")

    currentIteration += 1
    logging.info('Sleeping until next iteration')
    time.sleep((60 / hourSplit) * 60) 
    logging.info('Done sleeping')
        
logging.info('Completed %d iterations' % currentIteration)
