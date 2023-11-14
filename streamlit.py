import googleapiclient.discovery
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import re

#To connect with youtube

api_key="AIzaSyCWS6VXLAgSmZRriSRXdIcJBPaG8Y-_FXg"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)


#get channel information

def get_channel_details(channel_id):
    
    request = youtube.channels().list(part = "snippet,contentDetails,Statistics",id = channel_id)
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                         Channel_Id=i['id'],
                         Subscription_Count=i['statistics']['subscriberCount'],
                         Total_Videos=i['statistics']['videoCount'],
                         Channel_Views=i['statistics']['viewCount'],
                         Channel_Description=i['snippet']['description'],
                         Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])

    return data


# get Playlist details

def get_playlist_details(channel_id):

    next_page_token=None
    playlist_data=[]

    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,maxResults=50,pageToken=next_page_token)
        response = request.execute()

        for i in response['items']:
            data=dict(Playlist_Id=i['id'],
                     Title=i['snippet']['title'],
                     Channel_Id=i['snippet']['channelId'],
                     Channel_Name=i['snippet']['channelTitle'],
                     Published_At=i['snippet']['publishedAt'],
                     Video_Count=i['contentDetails']['itemCount'])
            playlist_data.append(data)
            
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data



#get video ids

def get_video_ids(channel_id):
    video_ids = []

    # get Uploads playlist id
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        response = youtube.playlistItems().list(part = 'snippet',playlistId = playlist_id, maxResults = 50,pageToken = next_page_token).execute()
        
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
        
    return video_ids

#  Function to convert Duration

def convert_duration(duration_str):
    try:
        # Use regular expression to extract hours, minutes, and seconds
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        hours, minutes, seconds = map(int, match.groups(default=0))

        # Create a timedelta object
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        # Format the duration as HH:MM:SS
        return "{:02}:{:02}:{:02}".format(duration.seconds // 3600, (duration.seconds // 60) % 60, duration.seconds % 60)
    except:
        return "00:00:00"



#get video information

def get_video_details(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id= video_id)
        response = request.execute()

        for i in response['items']:
            data=dict(Video_Name=i['snippet']['title'],
                       Channel_Name=i['snippet']['channelTitle'],
                       Channel_Id=i['snippet']['channelId'],
                       Video_Id=i['id'],
                       Video_Description=i['snippet'].get('description'),
                       Title=i['snippet']['title'],
                       Tags=" ".join(i['snippet'].get('tags','notags')),
                       Published_At=i['snippet']['publishedAt'],
                       View_Count=i['statistics']['viewCount'],
                       Like_Count=i['statistics']['likeCount'],
                       Favourite_Count=i['statistics']['favoriteCount'],
                       Comment_Count=i['statistics'].get('commentCount'),
                       Duration=convert_duration(i['contentDetails']['duration']),
                       Thumbnails=i['snippet']['thumbnails']['default']['url'],
                       Caption_Status=i['contentDetails']['caption'])
            video_data.append(data)
    return video_data



# To get comment details

def get_comment_details(video_ids):
    comment_data=[]
    try:
        for comments in video_ids:
            request = youtube.commentThreads().list(part="snippet",videoId=comments,maxResults=50)
            response = request.execute()

            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                    Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Display=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    
    return comment_data



# MongoDB connection

client = pymongo.MongoClient("mongodb+srv://Jose2000:Jose2000@cluster0.9kw7ejt.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube"]


# Uploading Youtube data details to MongoDB

def channel_ids(channel_id):
    channel_details = get_channel_details(channel_id)
    playlist_details = get_playlist_details(channel_id)
    video_ids = get_video_ids(channel_id)
    video_details = get_video_details(video_ids)
    comment_details = get_comment_details(video_ids)
    coll = db["data"]
    coll.insert_one({"channel_information":channel_details,"playlist_information":playlist_details,"video_information":video_details,
                     "comment_information":comment_details})
    
    return "Uploaded to MongoDB successfully"


# SQL connection and creating database

mydb=mysql.connector.connect(host='127.0.0.1',
                            user="root",
                            password="",
                            port=3306)
cursor=mydb.cursor()

#cursor.execute("drop database if exists youtube")
cursor.execute("create database if not exists youtube")



 # channel details to SQL

def channels_sql():
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="",
                                database= "youtube",
                                port = "3306")
        cursor = mydb.cursor(buffered=True)
        
        cursor.execute("USE youtube")

        cursor.execute("DROP TABLE if exists channel_table")
        cursor.execute('''CREATE TABLE IF NOT EXISTS channel_table (Channel_Name VARCHAR(255),
                                                        Channel_Id VARCHAR(255) primary key,
                                                        Subscription_Count BIGINT,
                                                        Total_Videos INT,
                                                        Channel_Views BIGINT,
                                                        Channel_Description TEXT,
                                                        Playlist_id VARCHAR(255))''') 

        channel_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"channel_information":1}):
                channel_list.append(i["channel_information"])
        df=pd.DataFrame(channel_list)

        for index,row in df.iterrows():
                sql = '''INSERT INTO channel_table(Channel_Name,
                                        Channel_Id,
                                        Subscription_Count,
                                        Total_Videos,
                                        Channel_Views,
                                        Channel_Description,
                                        Playlist_id)VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        
                values =(
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Subscription_Count'],
                        row['Total_Videos'],
                        row['Channel_Views'],
                        row['Channel_Description'],
                        row['Playlist_Id'])
                
                cursor.execute(sql,values)
                mydb.commit()


# playlist details to SQL

def playlists_sql():
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="",
                                database= "youtube",
                                port = "3306")
        cursor = mydb.cursor(buffered=True)
        
        cursor.execute("USE youtube")

        cursor.execute("DROP TABLE if exists playlist_table")
        cursor.execute('''CREATE TABLE if not exists playlist_table(Playlist_Id VARCHAR(255) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        Published_At timestamp,
                                                        Video_Count int)''') 

        playlist_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"playlist_information":1}):
                for j in range(len(i['playlist_information'])):
                        playlist_list.append(i['playlist_information'][j])
        df1=pd.DataFrame(playlist_list)

        for index,row in df1.iterrows():
                sql = '''INSERT INTO playlist_table(Playlist_Id,
                                        Title,
                                        Channel_Id,
                                        Channel_Name,
                                        Published_At,
                                        Video_Count)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''
                
                values =(
                        row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['Published_At'],
                        row['Video_Count'])
        
                cursor.execute(sql,values)
                mydb.commit()



# video details to SQL

def videos_sql():
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="",
                                database= "youtube",
                                port = "3306")
        cursor = mydb.cursor(buffered=True)
        
        cursor.execute("USE youtube")

        cursor.execute("DROP TABLE if exists video_table")
        cursor.execute('''CREATE TABLE if not exists video_table(Video_Name VARCHAR(255),
                                                                        Channel_Name varchar(100),
                                                                        Channel_Id varchar(100),
                                                                        Video_Id varchar(100)  primary key,
                                                                        Video_Description text,
                                                                        Title text,
                                                                        Tags text,
                                                                        Published_At timestamp,
                                                                        View_Count bigint,
                                                                        Like_Count bigint,
                                                                        Favourite_Count int,
                                                                        Comment_Count int,
                                                                        Duration varchar(50),
                                                                        Thumbnails varchar(255),
                                                                        Caption_Status varchar(100))''') 
        video_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"video_information":1}):
                for j in range(len(i['video_information'])):
                        video_list.append(i['video_information'][j])
        df2=pd.DataFrame(video_list)


        for index,row in df2.iterrows():
                sql = '''INSERT INTO video_table(Video_Name,
                                                Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Video_Description,
                                                Title,
                                                Tags,
                                                Published_At,
                                                View_Count,
                                                Like_Count,
                                                Favourite_Count,
                                                Comment_Count,
                                                Duration,
                                                Thumbnails,
                                                Caption_Status)
                                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                values =(
                        row['Video_Name'],
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Video_Description'],
                        row['Title'],
                        row['Tags'],
                        row['Published_At'],
                        row['View_Count'],
                        row['Like_Count'],
                        row['Favourite_Count'],
                        row['Comment_Count'],
                        row['Duration'],
                        row['Thumbnails'],
                        row['Caption_Status'])
                cursor.execute(sql,values)
                mydb.commit()


# comment details to SQL

def comments_sql():
        mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="",
                                database= "youtube",
                                port = "3306")
        cursor = mydb.cursor(buffered=True)
        
        cursor.execute("USE youtube")

        cursor.execute("DROP TABLE if exists comment_table")
        cursor.execute('''CREATE TABLE if not exists comment_table(Comment_Id VARCHAR(255) primary key,
                                                        Video_Id varchar(100),
                                                        Comment_Display text,
                                                        Comment_Author varchar(100),
                                                        Comment_Published timestamp)''') 

        comment_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"comment_information":1}):
                for j in range(len(i['comment_information'])):
                        comment_list.append(i['comment_information'][j])
        df3=pd.DataFrame(comment_list)

        for index,row in df3.iterrows():
                sql = '''INSERT INTO comment_table(Comment_Id,
                                        Video_Id,
                                        Comment_Display,
                                        Comment_Author,
                                        Comment_Published)
                                        VALUES(%s,%s,%s,%s,%s)'''
                
                values =(
                        row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Display'],
                        row['Comment_Author'],
                        row['Comment_Published'])
        
                cursor.execute(sql,values)
                mydb.commit()


def tables():
    channels_sql()
    playlists_sql()
    videos_sql()
    comments_sql()

    return "Migrated to SQL successfully"



def st_channel_table():
        ch_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"channel_information":1}):
                ch_list.append(i["channel_information"])
        ch_table=st.dataframe(ch_list)

        return ch_table


def st_playlist_table():
        pl_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"playlist_information":1}):
                for j in range(len(i['playlist_information'])):
                        pl_list.append(i['playlist_information'][j])
        pl_table=st.dataframe(pl_list)

        return pl_table

def st_video_table():
        vi_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"video_information":1}):
                for j in range(len(i['video_information'])):
                        vi_list.append(i['video_information'][j])
        vi_table=st.dataframe(vi_list)

        return vi_table



def st_comment_table():
        cm_list=[]
        db=client.youtube
        coll=db.data
        for i in coll.find({},{"_id":0,"comment_information":1}):
                for j in range(len(i['comment_information'])):
                        cm_list.append(i['omment_information'][j])
        cm_table=st.dataframe(cm_list)

        return cm_table



with st.sidebar:
        choice = option_menu(None, ["Home","Upload Data to MongoDB","SQL Data Warehouse","Channel queries"], 
                           icons=["house-door-fill"],
                           default_index=1,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "25px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "25px"},
                                   "container" : {"max-width": "8000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
if choice == "Home":
        st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
        
        st.write("YouTube is the world's most popular video-sharing platform, with over 2 billion active users. It is a valuable source of data for businesses, researchers, and individuals.")
        st.write("## :blue[Benefits]")
        st.write("This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.") 
        st.write("## :blue[Skills Take Away]")
        st.write("1. Python scripting")
        st.write("2. Data Collection")
        st.write("3. API integration")
        st.write("4. Data Management using MongoDB (Atlas)")
        st.write("5. SQL")
        st.write("6. Streamlit")



if choice == "Upload Data to MongoDB":
    channel_id=st.text_input("Enter Channel id")

    if st.button("STORE DATA to MongoDB"):
            ch_ids = []
            db = client["youtube"]
            coll = db["data"]
            for ch_data in coll.find({},{"_id":0,"channel_information":1}):
                ch_ids.append(ch_data["channel_information"]["Channel_Id"])

            if channel_id in ch_ids:
                st.success("Given Channel Id already exists")
            else:
                insert = channel_ids(channel_id)
            st.success(insert)

if choice == "SQL Data Warehouse":
    if st.button(":blue[MIGRATE DATA to SQL]"):
        Tables = tables()
        st.success(Tables)

    show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channel_table]",":green[playlist_table]",":green[video_table]",":green[comment_table]"))
    if show_table == ":green[channel_table]":
        st_channel_table()
    elif show_table == ":green[playlist_table]":
        st_playlist_table()
    elif show_table ==":green[video_table]":
        st_video_table()
    elif show_table == ":green[comment_table]":
        st_comment_table()


if choice == "Channel queries":
    mydb = mysql.connector.connect(host="127.0.0.1",
                                    user="root",
                                    password="",
                                    database= "youtube",
                                    port = "3306")
    cursor = mydb.cursor(buffered=True)
        
    question = st.selectbox(
        'Please Select Your Question',
        ('1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))


    if question=='1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("select Video_Name as videoname, Channel_Name as channelname from video_table")
        df1=pd.DataFrame(cursor.fetchall(),columns=['Video Name','Channel Name'])
        st.write(df1)

    elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("select Channel_Name as channelname, Total_Videos as totalvideos from channel_table order by Total_Videos desc")
        df2=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Total Videos'])
        st.write(df2)

    elif question=='3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("select Channel_Name as channelname,  View_Count as views , Video_Name as videoname from video_table order by View_Count desc limit 10")
        df3=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Views','Video Name'])
        st.write(df3)

    elif question=='4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("select Video_Name as videoname, Comment_Count as comments from video_table")
        df4=pd.DataFrame(cursor.fetchall(),columns=['Video Name','Comments'])
        st.write(df4)

    elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("select Video_Name as videoname, Like_Count as likes, Channel_Name as channelname from video_table order by Like_Count desc")
        df5=pd.DataFrame(cursor.fetchall(),columns=['Video Name','Likes', 'Channel Name'])
        st.write(df5)

    elif question=='6. What is the total number of likes for each video, and what are their corresponding video names?':
        cursor.execute("select Video_Name as videoname, Like_Count as likes from video_table")
        df6=pd.DataFrame(cursor.fetchall(),columns=['Video Name','Likes'])
        st.write(df6)

    elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("select Channel_Name as channelname, Channel_Views as totalviews from channel_table")
        df7=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Total Views'])
        st.write(df7)

    elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("select Channel_Name as channelname, Video_Name as videoname, Published_At as publishedat from video_table where extract(year from Published_At) = 2022")
        df8=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Video Name','Publishedat'])
        st.write(df8)
    elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("select Channel_Name as channelname, AVG(Duration)*60 as avgduration from video_table group by Channel_Name")
        df9=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','AvgDuration(mins)'])
        st.write(df9)

    elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("select Channel_Name as channelname, Comment_Count as comments, Video_Name as videoname from video_table order by Comment_Count desc")
        df10=pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Comments','Video Name'])
        st.write(df10)
