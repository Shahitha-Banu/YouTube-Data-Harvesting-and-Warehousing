from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from PIL import Image

#Connecting Youtube API
def connectAPI():
    api_key = "AIzaSyB1xc1JRmeIrOhkrfUZZjLUrooc9ZKiosc"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = connectAPI()

#Get channel information using channel ID and channels API
def getChannelInformation(channel_ID):
    response_1 = youtube.channels().list(
                   part="snippet,contentDetails,statistics",
                    id=channel_ID)
     
    channel_data = response_1.execute()

    for i in range(len(channel_data["items"])):
        channel_information = dict(
                    channel_id = channel_data['items'][i]["id"],
                    channel_name = channel_data['items'][i]['snippet']['title'],
                    channel_description = channel_data['items'][i]['snippet']['description'],
                    playlist_id = channel_data['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    view_count = channel_data['items'][i]['statistics']['viewCount'],
                    subscriber_count = channel_data['items'][i]['statistics']['subscriberCount'],
                    video_count = channel_data['items'][i]['statistics']['videoCount'])
        return channel_information

#Get video IDs using channel ID and playlistItems API
def getChannelVideoID(channel_ID):
    video_ids = []
    # get playlist id from uploads
    request = youtube.channels().list(id=channel_ID, 
                                  part='contentDetails').execute()
    playlist_id = request['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
         response_2 = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
         for i in range(len(response_2['items'])):
             video_ids.append(response_2['items'][i]['snippet']['resourceId']['videoId'])

         next_page_token = response_2.get('nextPageToken')
        
         if next_page_token is None:
            break
    return video_ids

#Get video information using video IDs and videos API
def getVideoInformation(video_IDs):
    video_data = []
    for video_id in video_IDs:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response_3 = request.execute()
        for item in response_3["items"]:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_ID = item['id'],
                        title = item['snippet']['title'],
                        tags = item['snippet'].get('tags'),
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        description = item['snippet']['description'],
                        published_date = item['snippet']['publishedAt'],
                        duration = item['contentDetails']['duration'],
                        views = item['statistics']['viewCount'],
                        likes = item['statistics'].get('likeCount'),
                        comments = item['statistics'].get('commentCount'),
                        favorite_count = item['statistics']['favoriteCount'],
                        definition = item['contentDetails']['definition'],
                        caption_status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#Get comments information using video IDs and commentThreads API
def getCommentInformation(video_IDs):
    comment_information = []
    try:
        for video_id in video_IDs:
            request = youtube.commentThreads().list(
                       part = "snippet",
                       videoId = video_id,
                       maxResults = 50)
            response_4 = request.execute()

            for item in response_4["items"]:
                data = dict(
                        comment_id = item["snippet"]["topLevelComment"]["id"],
                        video_ID = item["snippet"]["videoId"],
                        comment_text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                        comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        comment_published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                comment_information.append(data)
    except:
        pass
                
    return comment_information

#get playlist IDs using channel ID and playlists API
def getPlaylistInformation(channel_ID):
    playlist_data = []
    next_page_token = None
    next_page = True

    while next_page:
        request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_ID,
                    maxResults=50,
                    pageToken=next_page_token)
        
        response_5 = request.execute()

        for item in response_5['items']: 
            data = dict(
                    playlist_id = item['id'],
                    title = item['snippet']['title'],
                    channel_id = item['snippet']['channelId'],
                    channel_name = item['snippet']['channelTitle'],
                    published_at = item['snippet']['publishedAt'],
                    video_count = item['contentDetails']['itemCount'])
            playlist_data.append(data)

        next_page_token = response_5.get('nextPageToken')
        
        if next_page_token is None:
            next_page=False
    return playlist_data

#creating a connection to MongoDB and creating a youtube_data database
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtubeData"]


# upload the channel details to MongoDB server
def getChannelDetails(channel_ID):
    channelDetails = getChannelInformation(channel_ID)
    playlistDetails =  getPlaylistInformation(channel_ID)
    videoIDs = getChannelVideoID(channel_ID)
    videoDetails = getVideoInformation(videoIDs)
    commentDetails = getCommentInformation(videoIDs)

    #creating a collection and inserting the documents
  
    collection = db["channelDetails"]
    collection.insert_one({"channel_information":channelDetails,"playlist_information":playlistDetails,"video_information":videoDetails,
                     "comment_information":commentDetails})
    
    return "Successfully uploaded!!!"

#Creating a channel table in postgres
def createChannelTable():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="aslam7862",
            database= "youtubeData",
            port = "5432"
            )
    cursor = mydb.cursor()

    dropQuery = "drop table if exists channel"
    cursor.execute(dropQuery)
    mydb.commit()
    
    try:
        createQuery = '''create table if not exists channel(Channel_Id varchar(80) primary key,
                        Channel_Name varchar(100),
                        Channel_Description text,
                        Playlist_Id varchar(50),
                        Views bigint,
                        Subscription_Count bigint, 
                        Total_Videos int)'''
        cursor.execute(createQuery)
        mydb.commit()
    except:
        st.write("Channel Table alredy created")
        
    channel_list = []
    db = client["youtubeData"]
    collection_1 = db["channelDetails"]
    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
        
    df1 = pd.DataFrame(channel_list)
    
    for index,row in df1.iterrows():
        insertQuery = '''insert into channel(Channel_Id,
                                                    Channel_Name,
                                                    Channel_Description,
                                                    Playlist_Id,
                                                    Views,
                                                    Subscription_Count,
                                                    Total_Videos)
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
            
        values =(
                row['channel_id'],
                row['channel_name'],
                row['channel_description'],
                row['playlist_id'],
                row['view_count'],
                row['subscriber_count'],
                row['video_count'])
        try:                     
            cursor.execute(insertQuery,values)
            mydb.commit()    
        except:
            st.write("Channel values are already inserted")
            
#Creating a playlists table in postgres
def createPlaylistTable():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="aslam7862",
            database= "youtubeData",
            port = "5432"
            )
    cursor = mydb.cursor()
    dropQuery = "drop table if exists playlist"
    cursor.execute(dropQuery)
    mydb.commit()
    
    try:
        createQuery = '''create table if not exists playlist(playlist_id varchar(100) primary key,
                        title varchar(150), 
                        channel_id varchar(100), 
                        channel_name varchar(100),
                        published_at timestamp,
                        video_count int
                        )'''
        cursor.execute(createQuery)
        mydb.commit()
    except:
        st.write("Playlists Table alredy created")
        
    db = client["youtubeData"]
    collection_2 =db["channelDetails"]
    playlist_list = []
    for pl_data in collection_2.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])
    df2 = pd.DataFrame(playlist_list)
    
    for index,row in df2.iterrows():
        insertQuery = '''insert into playlist(playlist_id,
                                                    title,
                                                    channel_id,
                                                    channel_name,
                                                    published_at,
                                                    video_count)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['playlist_id'],
                row['title'],
                row['channel_id'],
                row['channel_name'],
                row['published_at'],
                row['video_count'])
        try:
            cursor.execute(insertQuery,values)
            mydb.commit()
        except:        
            st.write("Playlists values are already inserted")
            
#Creating a videos table in postgres

def createVideosTable():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="aslam7862",
                database= "youtubeData",
                port = "5432"
                )
    cursor = mydb.cursor()
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()
    createQuery = '''create table if not exists videos(
                        channel_name varchar(150),
                        channel_id varchar(100),
                        video_ID varchar(50) primary key, 
                        title varchar(150), 
                        tags text,
                        thumbnail varchar(225),
                        description text, 
                        published_date timestamp,
                        duration interval, 
                        views bigint, 
                        likes bigint,
                        comments int,
                        favorite_count int, 
                        definition varchar(10), 
                        caption_status varchar(50) 
                        )''' 
    try:                    
        cursor.execute(createQuery)             
        mydb.commit()
    except:
        st.write("Videos Table alrady created")
                
    video_list = []
    db = client["youtubeData"]
    collection_3 = db["channelDetails"]
    for vi_data in collection_3.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])
            
    df3 = pd.DataFrame(video_list)
    
    for index, row in df3.iterrows():
        insertQuery = '''insert into videos (channel_name,
                        channel_id,
                        video_ID, 
                        title, 
                        tags,
                        thumbnail,
                        description, 
                        published_date,
                        duration, 
                        views, 
                        likes,
                        comments,
                        favorite_count, 
                        definition, 
                        caption_status 
                        )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
        values = (
                    row['channel_name'],
                    row['channel_id'],
                    row['video_ID'],
                    row['title'],
                    row['tags'],
                    row['thumbnail'],
                    row['description'],
                    row['published_date'],
                    row['duration'],
                    row['views'],
                    row['likes'],
                    row['comments'],
                    row['favorite_count'],
                    row['definition'],
                    row['caption_status'])
        try:                        
            cursor.execute(insertQuery,values)
            mydb.commit()
        except:
            st.write("videos values already inserted in the table")
                
#Creating a comment table in postgres
def createCommentTable():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="aslam7862",
                database= "youtubeData",
                port = "5432"
                )
    cursor = mydb.cursor()
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()
    
    try:
        createQuery = '''create table if not exists comment(comment_id varchar(100) primary key,
                       video_id varchar(80),
                       comment_text text, 
                       comment_author varchar(150),
                       comment_published_at timestamp)'''
        cursor.execute(createQuery)
        mydb.commit()
        
    except:
        st.write("Commentsp Table already created")
                
    comment_list = []
    db = client["youtubeData"]
    collection_4 = db["channelDetails"]
    for comment_data in collection_4.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df4 = pd.DataFrame(comment_list)
    
    for index, row in df4.iterrows():
            insertQuery = '''insert into comment(comment_id,
                                      video_id ,
                                      comment_text,
                                      comment_author,
                                      comment_published_at)
                            values (%s, %s, %s, %s, %s) '''
            values = (
                row['comment_id'],
                row['video_ID'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published']
            )
            try:
                cursor.execute(insertQuery,values)
                mydb.commit()
            except:
                st.write()
                
#Tables creation in SQL
def createTables():
    createChannelTable()
    createPlaylistTable()
    createVideosTable()
    createCommentTable()
    return "Tables Created successfully"

#displaying the channel table in the streamlit application    
def showChannelTable():
    channel_list = []
    db = client["youtubeData"]
    collection_1 = db["channelDetails"] 
    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    channel_table = st.dataframe(channel_list)
    return channel_table

#displaying the playlist table in the streamlit application
def showPlaylistTable():
    playlist_list = []
    db = client["youtubeData"]
    collection_2 = db["channelDetails"] 
    for pl_data in collection_2.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                playlist_list.append(pl_data["playlist_information"][i])
    playlist_table = st.dataframe(playlist_list)
    return playlist_table

#displaying the videos table in the streamlit application
def showVideosTable():
    videos_list = []
    db = client["youtubeData"]
    collection_3 = db["channelDetails"]
    for vi_data in collection_3.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            videos_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(videos_list)
    return videos_table

#displaying the comment table in the streamlit application
def showCommentTable():
    comment_list = []
    db = client["youtubeData"]
    collection_4 = db["channelDetails"]
    for comment_data in collection_4.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    comment_table = st.dataframe(comment_list)
    return comment_table


#Designing of Streamlit Application
img1= Image.open(r"C:\Users\Admin\Documents\GUVI\postgresql.png")
img2 = Image.open(r"C:\Users\Admin\Documents\GUVI\mongodb.png")
img3 = Image.open(r"C:\Users\Admin\Documents\GUVI\streamlit.png")
str1 = ''':green[This approach involves building a simple UI with Streamlit,
retrieving data from the YouTube API, storing it in a MongoDB data lake,
migrating it to a SQL datawarehouse, querying the data warehouse with SQL,
and displaying the data in theStreamlit app.]'''
image_list = [img1,img2,img3]

with st.sidebar:
    st.header(":orange[About]")
    with st.container():
        st.caption(str1)
    st.header(":orange[Tools Used]")
    st.image(image_list, width=50)
st.subheader(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]",divider = "rainbow")

#Getting channel id from the user 
channel_id = st.text_input(":violet[Enter the Channel id]")

#Updation of channel data to MongoDB
if st.button("Collect and Store"):
    ch_id = []
    db = client["youtubeData"]
    collection = db["channelDetails"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_id.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_id:
        st.success("Channel details already exists")
    else:
        try:
            output = getChannelDetails(channel_id)
            st.success(output)
        except:
            st.write("Enter a valid channel ID")

#SQL Table creation from MongoDB to PostgreSQL           
if st.button("Generate Table"):
    display = createTables()
    st.success(display)

#Displaying the Created Tables in the application
tab1, tab2, tab3, tab4 = st.tabs(["ChannelTable", "PlaylistTable", "VideosTable", "CommentTable"])
with tab1:
    st.subheader("channelTable")
    showChannelTable()

with tab2:
    st.subheader("playlistTable")
    showPlaylistTable()
    
with tab3:
    st.subheader("videosTable")
    showVideosTable()
    
with tab4:
    st.subheader("commentTable")
    showCommentTable()

#creating a SQL Connection
mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="aslam7862",
                database= "youtubeData",
                port = "5432"
                )
cursor = mydb.cursor()

#Creating a drop-down box for choosing the query need to be viewed

question = st.selectbox(":violet[SQL Query Output need to displayed as table]",
    ('1.What are the names of all the videos and their corresponding channels?',
     '2.Which channels have the most number of videos, and how many videos do they have?',
     '3.What are the top 10 most viewed videos and their respective channels?',
     '4.How many comments were made on each video, and what are their corresponding video names?',
     '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7.What is the total number of views for each channel, and what are their corresponding channel names?',
     '8.What are the names of all the channels that have published videos in the year 2022?',
     '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10.Which videos have the highest number of comments, and what are their corresponding channel names?'),
     index = None, placeholder = "Select an option")

#SQL Query for question 1
if question == '1.What are the names of all the videos and their corresponding channels?':
    query1 = "select title as videos, channel_name as ChannelName from videos"
    cursor.execute(query1)
    mydb.commit()
    table1=cursor.fetchall()
    st.write(pd.DataFrame(table1, columns=["Video Title","Channel Name"]))

#SQL Query for question 2
elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select channel_name as channelName,total_videos as videosCount from channel order by total_videos desc"
    cursor.execute(query2)
    mydb.commit()
    table2=cursor.fetchall()
    st.write(pd.DataFrame(table2, columns=["Channel Name","Total Video Count"]))

#SQL Query for question 3
elif question == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select channel_name as channelName, views as views , title as videoTitle from videos 
                        where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    table3 = cursor.fetchall()
    st.write(pd.DataFrame(table3, columns = ["channel Name","No of Views","video title"]))

#SQL Query for question 4
elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select title as videoTitle, comments as commentNo from videos where comments is not null"
    cursor.execute(query4)
    mydb.commit()
    table4=cursor.fetchall()
    st.write(pd.DataFrame(table4, columns=["Video Title", "No of Comments"]))

#SQL Query for question 5
elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select channel_name as channelName, likes as likesCount, title as videoTitle from videos 
                       where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    table5 = cursor.fetchall()
    st.write(pd.DataFrame(table5, columns=["channel Name","like count","Video Title"]))

#SQL Query for question 6
elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = "select likes as likeCount,title as videoTitle from videos"
    cursor.execute(query6)
    mydb.commit()
    table6 = cursor.fetchall()
    st.write(pd.DataFrame(table6, columns=["Likes count","video title"]))

#SQL Query for question 7
elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select channel_name as channelName, views as channelviews from channel order by views"
    cursor.execute(query7)
    mydb.commit()
    table7=cursor.fetchall()
    st.write(pd.DataFrame(table7, columns=["channel Name","Total Views"]))

#SQL Query for question 8
elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select channel_name as channelName, published_Date as videoDate, title as videoTitle from videos 
                where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    table8=cursor.fetchall()
    st.write(pd.DataFrame(table8,columns=["Channel Name", "Published Date", "Video Title"]))

#SQL Query for question 9
elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "select channel_name as ChannelName, avg(duration) as averageDuration from videos group by channel_name"
    cursor.execute(query9)
    mydb.commit()
    table9=cursor.fetchall()
    st.write(pd.DataFrame(table9,columns=["Channel Name", "Average Video Duration"]))
    
#SQL Query for question 10
elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select channel_name as channelName,comments as commentsCount, title as videoTitle from videos 
                       where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    table10=cursor.fetchall()
    st.write(pd.DataFrame(table10, columns=['Channel Name','NO Of Comments','Video Title']))
