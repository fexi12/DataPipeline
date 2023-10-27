import urllib.parse
import logging
import requests;
from pprint import pprint;
import pandas;
from multiprocessing import Pool
import configparser
import os

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('Config/.env')

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
    base_url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/'

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


    pprint("---------------")
    pprint("Nhtsa vehicles")
    pprint(f"Retrieved {len(response_data)} vehicles")

    return response_data

#modelService

def modelService(dict_model_year:dict):
    response_data = [] 

    base_url= config['NHSA_Ratings']['NHTSA_URL_Ratings']+'/modelyear/'
    
    for dict in dict_model_year:
        url = f"{base_url}{dict['ModelYear']}/make/{dict['Make']}"
        #response = extract(url,'Results')
        response = extract(url,'Results')
        
        results = response['Results']
        if(len(results) > 0 ):

            response_data.extend([{'Make': item['Make'], 'ModelYear': item['ModelYear'], 'Model' : item['Model']} for item in results])

        else:
            logging.warning(f"No data retrieved from:{url}")

        # example one that don't have models in that year -> https://api.nhtsa.gov/SafetyRatings/modelyear/2010/make/BENTLEY
       

    # Response from extract are data frames , so i need it to combine
    #combined_df = pandas.concat(results, ignore_index=True)


    logging.info("---------------")
    logging.info("Nhtsa models")
    logging.info("Total retrieved :" , len(response_data))

    pprint("---------------")
    pprint("Nhtsa models")
    print(f"Total retrieved :" , len(response_data))
    return response_data

#modelYeardataService
def modelYeardataService():


    url = config['NHSA_Ratings']['NHTSA_URL_Ratings']


    data = extract(url,'Results')
    
    #df = pandas.DataFrame(data)['ModelYear']
    #logging.info(type(data))
    #logging.info(data)
    #df = data['ModelYear']
    #response_array = df.to_numpy()
    if len(data)<0:
        return []
    else:
        data_results = data['Results']
    if len(data_results) > 0:
        list_years = [data_results['ModelYear'] for data_results in data_results]
    else:
        return []

    logging.info("---------------")
    logging.info("Nhtsa model years")
    logging.info("Total retrieved :" , data['Count'])
    logging.info("Retrieved from :",  data_results[0]['ModelYear'], "-" , data_results[-1]['ModelYear'])

    pprint("---------------")
    pprint("Nhtsa model years")
    pprint(f"Total retrieved : {int(data.get('Count'))}" )
    pprint(f"Retrieved from :  {int(data_results[0]['ModelYear'])} -  {int(data_results[-1]['ModelYear'])}")
    
    

    
    return(list_years)

#makeService
def makeService(years:list):
    
    logging.info("Here")
    base_url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/'
    response_data = []

    for year in years:
        url = f"{base_url}{year}"

        response = extract(url,'Results')
        
        results = response['Results']

        if(len(results) > 0 ):
            response_data.extend([{'Make': item['Make'], 'ModelYear': item['ModelYear'] } for item in results])
        else:
            logging.warning(f"No data retrieved from:{url}")


    logging.info("---------------")
    logging.info("Nhtsa make")
    logging.info("Total retrieved :" , len(response_data))

    print("---------------")
    print("Nhtsa make")
    print(f"Total retrieved :  {(len(results))}")

    return response_data

import functools
#Teste Purpose
def extractThread(urlHTTP,normalizeOutput):

    processors = Pool(processes=os.cpu_count())

    extract_partial = functools.partial(extract, normalizeOutput=normalizeOutput)

    results = processors.map(extract_partial , urlHTTP)
    processors.close()
    processors.join()
    return results
    
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
            pprint(f"Error Connecting:{response.status_code}")
            logging.error("Error Connecting:",response.status_code)
            return []

        if len(responseData.get(normalizeOutput)) > 0:
            return responseData
        else :
            logging.warning(f"{url}, Didn't have results")
            return responseData

    except requests.exceptions.HTTPError as errh:
        logging.error("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        logging.error("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        logging.error("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        logging.error("OOps: Something Else",err)

        ''' #why i'm using normalize
        if 'Results' in responseData:
            df = pandas.json_normalize(responseData,normalizeOutput)
            return df
        else:
            logging.warning(f"{url}, Didn't have results")
            return []'''

def custom_url_encode(s):
    return s.replace('/', '$2F').replace(' ','$20')