# YouTube-Data-Harvesting-and-Warehousing

This project intends to provide users with the ability to access and analyse data from numerous YouTube channels.SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

Libraries to be imported:

1.googleapiclient.discovery

2.streamlit

3.psycopg2

4.pymongo

5.pandas

6.PIL

Approach:
1. Set up a Streamlit application:
	 A simple Streamlit application has been developed  where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse and get required details from the tables created through queries.

2. Connect to the YouTube API: 
	The Google API client library has to be created for Python to make requests to the API.

3. Store data in a MongoDB data lake:
	 Once the data has been retrived from the YouTube API, it is then stored in a MongoDB data lake.

4. Migrate data to a SQL data warehouse: 
	 PostgreSQL has been used to migrate the MongoDB data to Structured table.

5. Query the SQL data warehouse:
	 SQL queries are used to retrive the data for the given perticular scenario.

Disclaimer:
	 Respecting YouTube's terms and conditions, the data has been collected ethically and has not been misused anywhere during the entire process.
