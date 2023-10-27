
import configparser
import logging
import requests
import io
import zipfile
from datetime import datetime
import os

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

#config = configparser.ConfigParser()
#config.read('NHTSA\ManufacturerCommunications\config\.env')


def extract(config):

    
    extract_zip(config)
    extract_file(config)


def extract_zip(config):

    path_in = config['FILES']['IN']



    date_time= datetime.now().strftime("%Y-%m-%d")
    url = config['TSBS']['ZIP']
    dst = path_in+'\\'+ date_time + '-' + 'FLAT_TSBS.zip'
    r = requests.get(url)


    if len(r.content) > 0:

        logger.info(f"Retrieved content from url {url}")

        with open(dst,'wb') as f:
            f.write(r.content)

        unzip(r.content,config)
        logger.info("Response has content.")
    
    else:
        print(f"Response is empty from {url}")
        logger.info(f"Response is empty from {url}")

        

def extract_file(config):

    path_in = config['FILES']['IN']

    date_time= datetime.now().strftime("%Y-%m-%d")
    url = config['TSBS']['TXT']  
    dst = path_in+'\\' + date_time + '-' + 'TSBS.txt'

    r = requests.get(url)

    if len(r.content) > 0:

        with open(dst,'wb') as f:
            f.write(r.content)

        logger.info(f"Retrieved {len(r.content)} from url {url}")
    
    else:
        logger.info(f"Response is empty from {url}")
    

    logger.info(f"Retrieved {r.ok} from url {url}")


def unzip(r,config):

    path_in = config['FILES']['IN']
    zip_name = config['NAME']['ZIP']

    date_time= datetime.now().strftime("%Y-%m-%d")
    z = zipfile.ZipFile(io.BytesIO(r))
    z.extractall(config['FILES']['IN'])
    if not os.path.isfile(path_in + '\\' + date_time + '-' + zip_name):
        os.rename(path_in + '\\' + zip_name, path_in + '\\' + date_time + '-' + zip_name)
