import urllib.parse
import logging
import requests;
from pprint import pprint;
import pandas;
from multiprocessing import Pool
import configparser
import os
import concurrent.futures
from datetime import datetime 

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('NHTSA\Recalls\config\.env')


def getRecalls(models:dict):

    start_time = datetime.now() 

    response_data = []
    url_list = []
    base_url = config['NHSA_Recalls']['NHTSA_URL_Recalls']

    for dict in models:
            
        params = {
                    'model':custom_url_encode(dict['model']),
                    'modelYear': dict['modelYear'],
                    'make': dict['make']
                }
                    
        #response = extract(url,'Results')
        url_list.append((base_url, params))
        
    response = extractThread(url_list)

    for item in response:
        if item:
            if item.get('Count')> 0  :
                results = item.get('results')
                response_data.extend(results)

    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa Recalls")
    logger.info(f"Retrieved {len(response_data)} Recalls")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))

    return response_data

def getModels(dict_model_year:dict):

    start_time = datetime.now() 

    response_data = [] 

    base_url= config['NHSA_Recalls']['NHTSA_URL_Recalls_Model']
    
    for dict in dict_model_year:

        params = params = {
                            'issueType':'r',
                            'modelYear': dict['modelYear'],
                            'make': dict['make']
                            }


        response = extract(base_url, params=params)
        
        results = response['results']

        if(len(results) > 0 ):

            response_data.extend([{'make': item['make'], 'modelYear': item['modelYear'], 'model' : item['model']} for item in results])

        else:
            logger.warning(f"No data retrieved from:{base_url}")
     

    # Response from extract are data frames , so i need it to combine
    #combined_df = pandas.concat(results, ignore_index=True)
    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa models")
    logger.info(f"Total retrieved :" , len(response_data))
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))

    return response_data

def getModelYears():


    start_time = datetime.now() 

    url = config['NHSA_Recalls']['NHTSA_URL_Recalls_Year']
    params = {'issueType':'r'}

    data = extract(url,params)
    
    if len(data)<0:
        return []
    else:
        data_results = data['results']
    if len(data_results) > 0:
        list_years = [data_results['modelYear'] for data_results in data_results]
    else:
        return []


    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa model years")
    logger.info(f"Total retrieved : {int(data.get('count'))}" )
    logger.info(f"Retrieved from :  {int(data_results[0]['modelYear'])} -  {int(data_results[-1]['modelYear'])}")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
    

    
    return(list_years)

def getMakes(years:list):
    
    start_time = datetime.now()

    base_url = config['NHSA_Recalls']['NHTSA_URL_Recalls_Makes']
    response_data = []
    
    for year in years:
        url = f"{base_url}"

        params = params = {'issueType':'r',
                            'modelYear': year}
        
        data = extract(url,params)
        
        if len(data)<0:
            logger.warning(f"No data retrieved from:{url}")
            return []
        else:
            data_results = data['results']
        if len(data_results) > 0:
           response_data.extend([{'make': item['make'], 'modelYear': item['modelYear'] } for item in data_results])
        else:
            return []
        

    time_elapsed = datetime.now() - start_time 

    logger.info("---------------")
    logger.info("Nhtsa make")
    logger.info(f"Total retrieved :  {(len(response_data))}")
    logger.info('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))

    return response_data

import functools
#Teste Purpose
def extractThread(url_param_list):


    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        result_url = {executor.submit(extract, url, params) for url, params in url_param_list}

        concurrent.futures.wait(result_url)

        results = [result.result() for result in result_url]

    return results
    

def extract(urlHTTP:str,params:dict) -> list:



    try:
        url = urlHTTP
        header = {"Content-Type": "application/json",
              "Accept-Encoding" : "deflate"}
     
        response = requests.get(url,headers=header,params=params)

        if response.status_code == 200:
            responseData = response.json()
        else:
            logger.error(f"Error Connecting:{response.status_code}")
            return []

        if len(responseData.get('results')) > 0:
            return responseData
        else :
            logger.warning(f"{url}, Didn't have results")
            return responseData

    except requests.exceptions.HTTPError as errh:
        logger.error("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        logger.error("OOps: Something Else",err)


def custom_url_encode(s):
    return s.replace('/', '$2F').replace(' ','$20')