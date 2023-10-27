
# ETL Pipeline 📎

This is my private repository, inspired by a career-related challenge.
It is still in `Development` stage. in this project you can see an ETL Pipeline, it extracts data from different public API - [NHTSA](https://www.nhtsa.gov/nhtsa-datasets-and-apis) , this one having various sources of an API or a ZiP File.


The language used was python and its different modules were:
-    Python 3.9
-    pyspark
-    pandas
-    configparser
-    json
-    sqlalchemy
-    concurrent.futures




# Project Tree

<pre>
📦NHTSA
 ┣ 📂Complaints
 ┃ ┣ 📂config
 ┃ ┃ ┗ 📜config.ini
 ┃ ┣ 📂in
 ┃ ┣ 📂out
 ┃ ┣ 📜extract_complaints.py
 ┃ ┣ 📜load_complaints.py
 ┃ ┣ 📜main_complaints.py
 ┃ ┗ 📜transform_complaints.py
 ┣ 📂Investigations
 ┃ ┣ 📂config
 ┃ ┃ ┗ 📜.env
 ┃ ┣ 📂in
 ┃ ┣ 📂out
 ┃ ┣ 📜extract_investigations.py
 ┃ ┣ 📜load_investigations.py
 ┃ ┣ 📜main_investigations.py
 ┃ ┗ 📜transform_investigations.py
 ┣ 📂ManufacturerCommunications
 ┃ ┣ 📂config
 ┃ ┃ ┗ 📜.env
 ┃ ┣ 📂in
 ┃ ┣ 📂out
 ┣ 📂Ratings
 ┃ ┣ 📂config
 ┃ ┃ ┗ 📜.env
 ┃ ┣ 📂in
 ┃ ┣ 📂out
 ┃ ┣ 📜extract_ratings.py
 ┃ ┣ 📜load_ratings.py
 ┃ ┣ 📜main_ratings.py
 ┃ ┗ 📜transform_ratings.py
 ┗ 📂Recalls
 ┃ ┣ 📂config
 ┃ ┃ ┗ 📜.env
 ┃ ┣ 📂in
 ┃ ┣ 📂out
 ┃ ┣ 📜extract_recalls.py
 ┃ ┣ 📜load_recalls.py
 ┃ ┣ 📜main_recalls.py
 ┃ ┗ 📜transform_recalls.py
</pre>

Each sub-folders is referring to [NHTSA](https://www.nhtsa.gov/nhtsa-datasets-and-apis) and it's Dataset Source.

<pre>
To Start a Pipeline you need to execute the command -> ´´python .\main_*.py ´´
Main*.py initialize the pipeline giving it's proper order :
 - extract.*py -> transform.*.py -> load.*py
</pre>
## Extract.*py

  ```txt
  - Retrieve the JSON from the API or the ZIP from the URL
  - Upload the sources to /in
  ```
## Transform.*.py

  ```txt
  - Get the file from /in
  - Transform to DataFrame via pyspark
  - Do transformation to the data
  - Transform to Pandas DataFrame
  - Upload the df to /out
  ```

## Load.*.py

  ```txt
  - Get the DataFrame from Transform.*py
  - Upload to a Database 
  ```
## .env
  ```txt
  - It holds the variables
  ```

<!-- ROADMAP -->
## Roadmap

- [x] Change the request to multiThread request ( to improve (reduce) the time of extract )
- [x] Create a .env file for the folders
- [x] Transform data to the correct format
- [x] Replace Null Values to empty
- [x] Upload the DataFrame Locally
- [ ] Check the Data and it's transformation 
- [ ] Upload the DataFrame to MSQ Server
- [ ] Create DataFrame with index (missing someones)
- [ ] See if it's possible to create foreing keys in order to link the dataframes in the SQL

## Problems faced

- Understanding which data will be useful was challenging. I took an approach of retrieving all the data ( that was very high time consuming and delayed the development )
- Pipelines took too much time (hours) and made excessive requests to the API, leading to temporary API blocks.
- Limit on Storage to upload the SQL Lite DB and the ZIP and JSON on folder /in & /out
