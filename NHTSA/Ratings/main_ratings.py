import json
from extract_ratings import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import configparser

config = configparser.ConfigParser()
config.read('Config/.env')

#loadnsdataService
def main():
    
    
    #getOS
    #url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/2024/make/BMW/model/2 SERIES COUPE'
    #url = 'https://api.nhtsa.gov/SafetyRatings'
    #url = 'https://api.nhtsa.gov/SafetyRatings/modelyear/2013/make/ACURA'
    
   
    modelYear = modelYeardataService()
    
    make = makeService(modelYear)
    
    teste_make_year = [{'Make': 'ACURA', 'ModelYear': 2024},
 {'Make': 'ALFA', 'ModelYear': 2024},
 {'Make': 'AUDI', 'ModelYear': 2024},
 {'Make': 'BENTLEY', 'ModelYear': 2024},
 {'Make': 'BMW', 'ModelYear': 2024},
 {'Make': 'BRIGHTDROP', 'ModelYear': 2024},
 {'Make': 'BUICK', 'ModelYear': 2024},
 {'Make': 'CADILLAC', 'ModelYear': 2024},
 {'Make': 'CHEVROLET', 'ModelYear': 2024},
 {'Make': 'CHRYSLER', 'ModelYear': 2024},
 {'Make': 'DODGE', 'ModelYear': 2024},
 {'Make': 'FIAT', 'ModelYear': 2024},
 {'Make': 'FORD', 'ModelYear': 2024},
 {'Make': 'GENESIS', 'ModelYear': 2024},
 {'Make': 'GMC', 'ModelYear': 2024},
 {'Make': 'HONDA', 'ModelYear': 2024},
 {'Make': 'HYUNDAI', 'ModelYear': 2024},
 {'Make': 'INFINITI', 'ModelYear': 2024},
 {'Make': 'JAGUAR', 'ModelYear': 2024},
 {'Make': 'JEEP', 'ModelYear': 2024},
 {'Make': 'KIA', 'ModelYear': 2024},
 {'Make': 'LAND ROVER', 'ModelYear': 2024},
 {'Make': 'LEXUS', 'ModelYear': 2024},
 {'Make': 'LINCOLN', 'ModelYear': 2024},
 {'Make': 'LUCID', 'ModelYear': 2024},
 {'Make': 'MAZDA', 'ModelYear': 2024},
 {'Make': 'MERCEDES-BENZ', 'ModelYear': 2024},
 {'Make': 'MERCEDES-MAYBACH', 'ModelYear': 2024},
 {'Make': 'MINI', 'ModelYear': 2024}]



    model = modelService(teste_make_year)

    vehicleId = vehicleIdService(model)

   




    file_to_export = safetyRatings(vehicleId)


    df = pandas.DataFrame(file_to_export)

    df.to_excel(config['FILES']['IN']+'/NHTSA_Ratings.xlsx',index=False)



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