import urllib.parse
import logging
import requests;
from pprint import pprint;
import pandas;
from multiprocessing import Pool
import configparser
import os
import pathlib
from datetime import datetime 
import functools

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

config_path = pathlib.Path(__file__).parent.absolute() / "config/.env"
config = configparser.ConfigParser()
config.read(config_path)


def extractRatings():

    # modelYear()
    debug_list = [2024,2023]

    service_list = makeService(debug_list)
    
    if(len(service_list) == 0):
        logger.error("No data to process")
        return

    model_list = modelService(service_list)

    id_list = vehicleIdService(model_list)

#safetyRatings
def safetyRatings(vechileIDs:list):


    base_url = 'https://api.nhtsa.gov/SafetyRatings/VehicleId/'
    response_data = []

    for id in vechileIDs:
        url = f"{base_url}{id}"
        response = extract(url,'Results')
        
        if response.get('Count') > 0:
            response_data.extend(response.get('Results'))

    #combined_df = pandas.concat(results, ignore_index=True)
    
    pprint("---------------")
    pprint("Nhtsa safetyRatings")
    pprint(f"Total retrieved :  {int(len(response_data))}")

    return response_data

#vehicleIdService
def vehicleIdService(models:dict):

    response_data = []
    url_list = []
    base_url= config['NHTSA']['Service']
    start_time = datetime.now()

    for dict in models:
            
        encoded_model_name = custom_url_encode(dict['Model'])

        url = f"{base_url}{dict['ModelYear']}/make/{dict['Make']}/model/{encoded_model_name}"

        #response = extract(url,'Results')
        url_list.append(url)

        
    response = extractThread(url_list, 'Results')

    for item in response:
        if item.get('Count') > 0:
            results = item.get('Results')
            response_data.extend({vehicleId['VehicleId'] for vehicleId in results})

    time_elapsed = datetime.now() - start_time


    logger.info("---------------")
    logger.info("Nhtsa vehicles")
    logger.info(f"Total retrieved :" , {len(response_data)})
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))


    return response_data

#modelService
def modelService(dict_model_year:dict):

    start_time = datetime.now() 

    response_data = [] 

    base_url= config['NHTSA']['Service']
    
    for dict in dict_model_year:
        url = f"{base_url}{dict['ModelYear']}/make/{dict['Make']}"
        #response = extract(url,'Results')
        response = extract(url,'Results')
        
        results = response['Results']
        if(len(results) > 0 ):

            response_data.extend([{'Make': item['Make'], 'ModelYear': item['ModelYear'], 'Model' : item['Model']} for item in results])

        else:
            logger.warning(f"No data retrieved from:{url}")

        # example one that don't have models in that year -> https://api.nhtsa.gov/SafetyRatings/modelyear/2010/make/BENTLEY
       

    # Response from extract are data frames , so i need it to combine
    #combined_df = pandas.concat(results, ignore_index=True)

    time_elapsed = datetime.now() - start_time

    logger.info("---------------")
    logger.info("Nhtsa models")
    logger.info(f"Total retrieved : {len(response_data)}")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))

    return response_data

#modelYeardataService
def modelYear():

    start_time = datetime.now() 

    url = config['NHTSA']['Years']

    data = extract(url,'Results')
    
    if len(data)<0:
        return []
    else:
        data_results = data['Results']
    if len(data_results) > 0:
        list_years = [data_results['ModelYear'] for data_results in data_results]
    else:
        return []

    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa model years")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
    logger.info(f"Total retrieved : {int(data.get('Count'))}" )
    logger.info(f"Retrieved from :  {int(data_results[0]['ModelYear'])} - {int(data_results[-1]['ModelYear'])}")

    return(list_years)

#makeService
def makeService(years:list):
    
    base_url = base_url= config['NHTSA']['Service']
    response_data = []
    start_time = datetime.now() 

    for year in years:
        url = f"{base_url}{year}"

        response = extract(url,'Results')
        
        if len(response) == 0:
            logger.warning(f"No data retrieved from: {url}")
        
        else:
            results = response['Results']
            response_data.extend([{'Make': item['Make'], 'ModelYear': item['ModelYear'] } for item in results])

            

    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa make")
    logger.info(f"Total retrieved :  {len(response_data)}")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))

    return response_data

#Teste Purpose
def extractThread(urlHTTP,normalizeOutput):

    processors = Pool(processes=os.cpu_count())

    extract_partial = functools.partial(extract, normalizeOutput=normalizeOutput)

    results = processors.map(extract_partial , urlHTTP)
    processors.close()
    processors.join()
    return results
    #pprint(extract_partial)
    
#extract
def extract(urlHTTP,normalizeOutput) -> list:



    try:
        url = urlHTTP
        header = {"Content-Type": "application/json",
              "Accept-Encoding" : "deflate"}

        response = requests.get(url,headers=header)

        if response.status_code == 200:
            responseData = response.json()
        else:
            logger.error(f"Error Connecting: {response.status_code}")
            return []

        if len(responseData.get(normalizeOutput)) > 0:
            return responseData
        else :
            logger.warning(f"{url}, Didn't have results")
            return responseData

    except requests.exceptions.HTTPError as errh:
        logger.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logger.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logger.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logger.error(f"OOps: Something Else {err}")

        ''' #why i'm using normalize
        if 'Results' in responseData:
            df = pandas.json_normalize(responseData,normalizeOutput)
            return df
        else:
            logging.warning(f"{url}, Didn't have results")
            return []'''

def custom_url_encode(s):
    return s.replace('/', '$2F').replace(' ','$20')

if __name__ == '__main__':
    extractRatings()