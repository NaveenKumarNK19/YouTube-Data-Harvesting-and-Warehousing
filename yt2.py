# Importing necessary modules
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# API Key and details
api_key = "AIzaSyC3woT4DX12HBKw2OP9ME81etR2aVZ1l74"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

# Connection to PostgreSQL database
projectA = psycopg2.connect(host="localhost",user='postgres',password= "NK19",database= "youtube_data" )
cursor = projectA.cursor()

# Connection to MongoDB
mongo_db = pymongo.MongoClient("mongodb+srv://naveenkumargr70:OWrYxZLLLqtkkXxR@cluster0.avd1vee.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# Function to fetch channel details
def fetch_channel(youtube,channel_id):
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)


  response = request.execute()

  for item in response['items']:
    data={'channel_Name':item['snippet']['title'],
          'channel_Id':item['id'],
          'subscribers':item['statistics']['subscriberCount'],
          'views':item['statistics']['viewCount'],
          'total_Videos':item['statistics']['videoCount'],
          'playlist_Id':item['contentDetails']['relatedPlaylists']['uploads'],
          'channel_Description':item['snippet']['description']
    }

  return data

# Function to get details of playlists
def get_playlist_details(youtube,channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25)
    response = request.execute()
    all_data=[]
    for item in response['items']:
      data = {'PlaylistId':item['id'],
              'Title':item['snippet']['title'],
              'ChannelId':item['snippet']['channelId'],
              'ChannelName':item['snippet']['channelTitle'],
              'PublishedAt':item['snippet']['publishedAt'],
              'VideoCount':item['contentDetails']['itemCount']
              }
      all_data.append(data)

      next_page_token = response.get('nextpagetoken')

      while next_page_token is not None:
          
          request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=25)
          response = request.execute()

          for item in response['items']:
              data = {'PlaylistId':item['id'],
                      'Title':item['snippet']['title'],
                      'ChannelId':item['snippet']['channelId'],
                      'ChannelName':item['snippet']['channelTitle'],
                      'PublishedAt':item['snippet']['publishedAt'],
                      'VideoCount':item['contentDetails']['itemCount']
                      }
              all_data.append(data)

          next_page_token = response.get('nextpagetoken')
    return all_data

# Function to get video IDs
def get_videoIds(youtube, upload_id):
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=upload_id,
        maxResults=50
    )
    response = request.execute()
    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=upload_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids

# Function to get video details
def get_videoDetails (youtube,video_id):

    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id)
    response = request.execute()

    for video in response['items']:
      stats_needed = {'snippet': ['channelTitle','title','description','tags','publishedAt','channelId'],
                      'statistics': ['viewCount','likeCount','favouriteCount','commentCount'],
                      'contentDetails': ['duration','definition','caption']}
      video_info = {}
      video_info['video_id'] = video['id']



      for key in stats_needed.keys():
        for value in stats_needed[key]:
          try:
            video_info[value] = video [key][value]
          except KeyError:
            video_info[value] = None


    return video_info

# Function to get comment details
def comment_details(youtube, video_id):
    all_comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()

        for item in response['items']:
            data={'comment_id':item['snippet']['topLevelComment']['id'],
                  'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                  'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                  'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
            }


            all_comments.append(data)

    except:
        
        return 'Could not get comments for video '

    return all_comments

# MongoDB database connection and caching decorator
db = mongo_db["Youtube_data"]

@st.cache_data

# Function to get YouTube channel details
def channel_Details(channel_id):
  channel = fetch_channel(youtube,channel_id)
  collection = db["YoutubeChannels"]
  collection.insert_one(channel)

  playlist = get_playlist_details(youtube,channel_id)
  collection = db["Playlists"]
  for i in playlist:
    collection.insert_one(i)

  upload = channel.get('playlist_Id')
  videos = get_videoIds(youtube, upload)
  for i in videos:
    videoDetail = get_videoDetails (youtube,i)
    collection = db["Videos"]
    collection.insert_one(videoDetail)

    comment = comment_details(youtube,i)
    if comment != 'Could not get comments for video ':
      for i in comment:
        collection = db["Comments"]
        collection.insert_one(i)
  return ("Process for " + channel_id + " is completed")

# Function to create YouTube channel table in PostgreSQL
def youtube_channel_table():
    try:
        cursor.execute('''create table if not exists youtube_channel(channel_Name varchar(80),
                          channel_Id varchar(80) primary key,
                          subscribers bigint,
                          views bigint,
                          total_Videos int,
                          playlist_Id varchar(80),
                          channel_Description text)''')
        projectA.commit()
    except:
        projectA.rollback()
        
    collection1 = db["YoutubeChannels"]
    documents1 = collection1.find()
    data1 = list(documents1)
    yt1 = pd.DataFrame(data1)
        
    try:
        for _, row in yt1.iterrows():
            insert_query = '''
                INSERT INTO youtube_channel (channel_Name, channel_Id, subscribers,
                       views, total_Videos, playlist_Id, channel_Description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['channel_Name'],
                row['channel_Id'],
                row['subscribers'],
                row['views'],
                row['total_Videos'],
                row['playlist_Id'],
                row['channel_Description']
            )
            try:
                cursor.execute(insert_query, values)
                projectA.commit()
            except:
                projectA.rollback()
    except:
        st.write("Values already exists in the Youtube Channel table")

# Function to create playlists table in PostgreSQL
def playlist_table():
    try:
        cursor.execute('''create table if not exists playlists(PlaylistId varchar(80) primary key,
                          Title varchar(80),
                          ChannelId varchar(80),
                          ChannelName varchar(80),
                          PublishedAt timestamp,
                          VideoCount int )''')
        projectA.commit()
    except:
        projectA.rollback()
        
    collection2 = db["Playlists"]
    documents2 = collection2.find()
    data2 = list(documents2)
    yt2 = pd.DataFrame(data2)
        
    try:
        for _, row in yt2.iterrows():
            insert_query = '''
                INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName,
                       PublishedAt, VideoCount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']

            )
            try:
                cursor.execute(insert_query, values)
                projectA.commit()
            except:
                projectA.rollback()
    except:
        st.write("Values already exists in the Playlists table")

# Function to create videos table in PostgreSQL
def videos_table():
    try:
        cursor.execute('''create table if not exists videos(video_id varchar(80) primary key,
                          channelTitle varchar(200),
                          title text,
                          description text,
                          tags text,
                          publishedAt timestamp,
                          channelId varchar(80),
                          viewCount bigint,
                          likeCount bigint,
                          favouriteCount int,
                          commentCount int,
                          duration varchar(20),
                          definition varchar(10),
                          caption varchar(10) )''')
                        
        projectA.commit()
    except:
        projectA.rollback()
    
    collection3 = db["Videos"]
    documents3 = collection3.find()
    data3 = list(documents3)
    yt3 = pd.DataFrame(data3)

    try:
        for _, row in yt3.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle, title, description, tags,
                       publishedAt, channelId, viewCount, likeCount, favouriteCount, commentCount, duration,
                       definition, caption)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['channelId'],
                row['viewCount'],
                row['likeCount'],
                row['favouriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption']


            )
            try:
                cursor.execute(insert_query, values)
                projectA.commit()
            except:
                projectA.rollback()
    except:
        st.write("Values already exists in the Videos table")

# Function to create comments table in PostgreSQL
def comment_table():
    try:
        cursor.execute('''create table if not exists comments(comment_id varchar(80) primary key,
                          comment_txt text,
                          videoId varchar(80),
                          published_at timestamp)''')
        projectA.commit()
    except:
        projectA.rollback()
        
    collection4 = db["Comments"]
    documents4 = collection4.find()
    data4 = list(documents4)
    yt4 = pd.DataFrame(data4)
        
    try:
        for _, row in yt4.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId,
                       published_at)
                VALUES (%s, %s, %s, %s)

            '''
            values = (
                row['comment_id'],
                row['comment_txt'],
                row['videoId'],
                row['published_at']


            )
            try:
                cursor.execute(insert_query, values)
                projectA.commit()
            except:
                projectA.rollback()
    except:
        st.write("Values already exists in the Comments table")

# Function to create all necessary tables in PostgreSQL
def tables():
    youtube_channel_table()
    playlist_table()
    videos_table()
    comment_table()
    return ("Migration Done")

# Function to display YouTube channel data in Streamlit
def display_youtube_channel():
    try:
        cursor.execute("select * from youtube_channel;")
        a = cursor.fetchall()
        a = st.dataframe(a)
        return a
    except:
        projectA.rollback()
        cursor.execute("select * from youtube_channel;")
        a = cursor.fetchall()
        a = st.dataframe(a)
        return a

# Function to display playlist data in Streamlit
def display_playlist():
    try:
        cursor.execute("select * from playlists;")
        b = cursor.fetchall()
        b = st.dataframe(b)
        return b
    except:
        projectA.rollback()
        cursor.execute("select * from playlists;")
        b = cursor.fetchall()
        b = st.dataframe(b)
        return b

# Function to display video data in Streamlit
def display_videos():
    try:
        cursor.execute("select * from videos;")
        c = cursor.fetchall()
        c = st.dataframe(c)
        return c
    except:
        projectA.rollback()
        cursor.execute("select * from videos;")
        c = cursor.fetchall()
        c = st.dataframe(c)
        return c

# Function to display comment data in Streamlit
def display_comments():
    try:
        cursor.execute("select * from comments;")
        d = cursor.fetchall()
        d = st.dataframe(d)
        return d
    except:
        projectA.rollback()
        cursor.execute("select * from comments;")
        d = cursor.fetchall()
        d = st.dataframe(d)
        return d

# Various analytical queries for YouTube data
def one():
    cursor.execute('''select title as Videos, channeltitle as channelName from videos;''')
    projectA.commit()
    q1=cursor.fetchall()
    q1=st.dataframe(q1)
    return q1

def two():
    cursor.execute('''select channel_Name as ChannelName, total_Videos as No_of_Videos from youtube_channel order by total_Videos desc limit                       1;''')
    projectA.commit()
    q2=cursor.fetchall()
    q2=st.dataframe(q2)
    return q2

def three():
    cursor.execute('''select viewCount as Views, channelTitle as ChannelName, title as Name from videos where viewCount is not null order by                       viewCount desc limit 10;''')
    projectA.commit()
    q3=cursor.fetchall()
    q3=st.dataframe(q3)
    return q3

def four():
    cursor.execute('''select title as Name, channelTitle as ChannelName, commentCount as No_of_Comments from videos
                      where commentCount is not null;''')
    projectA.commit()
    q4=cursor.fetchall()
    q4=st.dataframe(q4)
    return q4

def five():
    cursor.execute('''select title as Video, channelTitle as ChannelName, likeCount as Likes from videos where likeCount is not null order                         by likeCount desc;''')
    projectA.commit()
    q5=cursor.fetchall()
    q5=st.dataframe(q5)
    return q5

def six():
    cursor.execute('''select channelTitle as ChannelName, title as Name, likeCount as Likes from videos;''')
    projectA.commit()
    q6=cursor.fetchall()
    q6=st.dataframe(q6)
    return q6

def seven():
    cursor.execute('''select channel_Name as ChannelName, views as ChannelViews from youtube_channel;''')
    projectA.commit()
    q7=cursor.fetchall()
    q7=st.dataframe(q7)
    return q7

def eight():
    cursor.execute('''select channelTitle as ChannelName, title as Name, publishedAt as ReleasedOn from videos where extract(year from                             publishedAt) = 2022;''')
    projectA.commit()
    q8=cursor.fetchall()
    q8=st.dataframe(q8)
    return q8

def ten():
    cursor.execute('''select title as Name, channelTitle as ChannelName, commentCount as Comments from videos
                      where commentCount is not null order by commentCount desc;''')
    projectA.commit()
    q10=cursor.fetchall()
    q10=st.dataframe(q10)
    return q10

# Streamlit interface
st.title("YOUTUBE DATA HARVESTING")
st.caption("Get Datas from the selected channel")

getting_input = st.text_input("Enter the channel ID")
st.write("You selected:", getting_input)

# Button to fetch and store data
if st.button("Fetch and store data"):
    output = channel_Details(getting_input)
    st.write(output)

# Button to migrate data
st.write("Click here to Migrate Data")
if st.button("Migrate"):
    display = tables()
    st.write(display)

# Dropdown to select table to display
frames = st.selectbox(
    "Select table",
    ('None','Youtube Channel','Playlists','Videos','Comments'))
st.write('You selected: ',frames)

# Display selected table
if frames=='None':
    st.write("Select table")
elif frames=='Youtube Channel':
    display_youtube_channel()
elif frames=='Playlists':
    display_playlist()
elif frames=='Videos':
    display_videos()
elif frames=='Comments':
    display_comments()
    
# Dropdown for various analytical queries
query = st.selectbox(
        "Channel Analysis",
        ('None','Names of all the videos and their corresponding channels', 'Channel having the most number of videos',
         'Top 10 most viewed videos', 'Number of Comments in each video', 'Videos with Highest Likes' ,'Likes of all videos', 
         'Total number of views for each channel', 'Names of the channels that have published videos in the year 2022',
         'Videos with highest number of comments'))

st.write('You selected: ',query)

# Display results of selected query
if query=='None':
    st.write("Select table")
elif query=='Names of all the videos and their corresponding channels':
    one()
elif query=='Channel having the most number of videos':
    two()
elif query=='Top 10 most viewed videos':
    three()
elif query=='Number of Comments in each video':
    four()
elif query=='Videos with Highest Likes':
    five()
elif query=='Likes of all videos':
    six()
elif query=='Total number of views for each channel':
    seven()
elif query=='Names of the channels that have published videos in the year 2022':
    eight()
elif query=='Videos with highest number of comments':
    ten()