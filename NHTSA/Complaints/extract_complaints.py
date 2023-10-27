
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




def extract(config):

    extract_zip(config)


def extract_zip(config):

    
    path_in = config['FILES']['IN']

    date_time= datetime.now().strftime("%Y-%m-%d")
    url = config['SINGLE-FILE']['ZIP']
    dst = path_in+'\\'+ date_time + '-' + 'FLAT_CMPL.zip'
    r = requests.get(url)

    #print(print(f"}"))
    if len(r.content) > 0:

        print(f"Retrieved content from url {url}")

        with open(dst,'wb') as f:
            f.write(r.content)

        unzip(r.content,config)
        logger.info("Response has content.")
    
    else:
        print(f"Response is empty from {url}")
        logger.info(f"Response is empty from {url}")

        

def unzip(r,config):

    path_in = config['FILES']['IN']
    zip_name = config['NAME']['ZIP']

    date_time= datetime.now().strftime("%Y-%m-%d")
    z = zipfile.ZipFile(io.BytesIO(r))
    z.extractall(config['FILES']['IN'])
    if not os.path.isfile(path_in + '\\' + date_time + '-' + zip_name):
        os.rename(path_in + '\\' + zip_name, path_in + '\\' + date_time + '-' + zip_name)
