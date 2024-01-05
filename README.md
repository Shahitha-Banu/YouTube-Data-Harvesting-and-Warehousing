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

Work flow:
1. The user has to give a valid channel ID in the given text box. In the case of an invalid channel ID a warning message will be displayed. Also for already added channel details, a message will be displayed accordingly.

2. On click of the button "collect and store", the data is fetched from the Youtube using the API key and it is stored in the MongoDB repository. Either if the channel ID given is already stored, or for an invalid channel ID a proper message will be popped.

3. On click of the button "Generate", the channel table, playlist table, videos table and comment table will be created.

4. The entire table can be viewed using the tab created for the purpose.

5. The recommended 10 queries has been created as a drop down list, the user can select any query to view the result.

Disclaimer:
	 Respecting YouTube's terms and conditions, the data has been collected ethically and has not been misused anywhere during the entire process.
