import json
from extract_ratings import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import configparser
import extract_ratings as extract


config = configparser.ConfigParser()
config.read('\\NHTSA\\Ratings\\config\\.env')

def main():
    
    
    #getOS
    #url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/2024/make/BMW/model/2 SERIES COUPE'
    #url = 'https://api.nhtsa.gov/SafetyRatings'
    #url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/2013/make/ACURA'
    
    extract()
   
    
   
    #make = makeService(modelYear)
    
    #model = modelService(teste_make_year)

    #vehicleId = vehicleIdService(model)

    #file_to_export = safetyRatings(vehicleId)

    #df = pandas.DataFrame(file_to_export)

    #df.to_excel(config['FILES']['IN']+'/NHTSA_Ratings.xlsx',index=False)


    #pprint(type(file_to_export))

    #print(df.head())
    #print(df.count())
    #print(df[df.columns[0]].count())
    
    #corrected_df = correctNull(df)
    
    




    #load(df,'NHTSA_Ratings')
    #load(corrected_df,'NHTSA_Ratings','NHTSA.db')


    #return(file_to_export)

if __name__ == '__main__':
    main()