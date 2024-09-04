# Importing necessary modules
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from datetime import timedelta

# API Key and details
api_key = "Api_key"
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
collection = db['YoutubeChannels']

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
        st.write(pd.DataFrame(a, columns=['Channel Name', 'Chennal id', 'Subscribers', 'Views', 'Total Videos', 'Playlist id', 'Channel Description']))
    except:
        projectA.rollback()
        cursor.execute("select * from youtube_channel;")
        a = cursor.fetchall()
        st.write(pd.DataFrame(a, columns=['Channel Name', 'Chennal id', 'Subscribers', 'Views', 'Total Videos', 'Playlist id', 'Channel Description']))

# Function to display playlist data in Streamlit
def display_playlist():
    try:
        cursor.execute("select * from playlists;")
        b = cursor.fetchall()
        st.write(pd.DataFrame( b, columns=['Playlist id', 'Title', 'Channel id', 'Channel Name', 'Published Date', 'Video Count']))
    except:
        projectA.rollback()
        cursor.execute("select * from playlists;")
        b = cursor.fetchall()
        st.write(pd.DataFrame( b, columns=['Playlist id', 'Title', 'Channel id', 'Channel Name', 'Published Date', 'Video Count']))

# Function to display video data in Streamlit
def display_videos():
    try:
        cursor.execute("select * from videos;")
        c = cursor.fetchall()
        st.write(pd.DataFrame( c, columns=['Video id', 'Channel Title', 'Title', 'Description', 'Tags', "Published Date", "Channel id", "View count", 'Like count', 'Favourite count', 'Comment Count', 'Duration', 'Definition', 'Caption']))
    except:
        projectA.rollback()
        cursor.execute("select * from videos;")
        c = cursor.fetchall()
        st.write(pd.DataFrame( c, columns=['Video id', 'Channel Title', 'Title', 'Description', 'Tags', "Published Date", "Channel id", "View count", 'Like count', 'Favourite count', 'Comment Count', 'Duration', 'Definition', 'Caption']))

# Function to display comment data in Streamlit
def display_comments():
    try:
        cursor.execute("select * from comments;")
        d = cursor.fetchall()
        st.write(pd.DataFrame( d, columns=['Comment id', 'Comment Text', 'Video id', 'Published Date']))
    except:
        projectA.rollback()
        cursor.execute("select * from comments;")
        d = cursor.fetchall()
        st.write(pd.DataFrame( d, columns=['Comment id', 'Comment Text', 'Video id', 'Published Date']))

# Various analytical queries for YouTube data
def one():
    cursor.execute('''select title, channeltitle from videos;''')
    projectA.commit()
    q1=cursor.fetchall()
    st.write(pd.DataFrame(q1, columns=['Videos','Channel Name']))

def two():
    cursor.execute('''select channel_Name , total_Videos from youtube_channel order by total_Videos desc limit 1;''')
    projectA.commit()
    q2=cursor.fetchall()
    st.write(pd.DataFrame(q2, columns=['Channel Name','Video Count']))

def three():
    cursor.execute('''select title , channelTitle , viewCount from videos where viewCount is not null order by viewCount desc limit 10;''')
    projectA.commit()
    q3=cursor.fetchall()
    st.write(pd.DataFrame(q3, columns=['Video Title','Channel Name','Views Count']))

def four():
    cursor.execute('''select title , channelTitle , commentCount from videos where commentCount is not null;''')
    projectA.commit()
    q4=cursor.fetchall()
    st.write(pd.DataFrame(q4, columns=['Video Title','Channel Name','Comments Count']))

def five():
    cursor.execute('''select title , channelTitle , likeCount from videos where likeCount is not null order by likeCount desc;''')
    projectA.commit()
    q5=cursor.fetchall()
    st.write(pd.DataFrame(q5, columns=['Video Title','Channel Name','Likes']))

def six():
    cursor.execute('''select title , channelTitle , likeCount from videos;''')
    projectA.commit()
    q6=cursor.fetchall()
    st.write(pd.DataFrame(q6, columns=['Video Title','Channel Name','Likes']))

def seven():
    cursor.execute('''select channel_Name , views from youtube_channel;''')
    projectA.commit()
    q7=cursor.fetchall()
    st.write(pd.DataFrame(q7, columns=['Channel Name','Channel Views']))

def eight():
    cursor.execute('''select channelTitle , title , publishedAt from videos where extract(year from publishedAt) = 2022;''')
    projectA.commit()
    q8=cursor.fetchall()
    st.write(pd.DataFrame(q8, columns=['Channel Name','Video Title','Released On']))

def parse_duration(duration_str):
    """Converts YouTube duration string (e.g., PT31S) to timedelta."""
    duration_str = duration_str[2:]  # Remove 'PT' from the beginning
    total_seconds = 0

    if 'H' in duration_str:
        hours, duration_str = duration_str.split('H')
        total_seconds += int(hours) * 3600

    if 'M' in duration_str:
        minutes, duration_str = duration_str.split('M')
        total_seconds += int(minutes) * 60

    if 'S' in duration_str:
        seconds = duration_str.replace('S', '')
        total_seconds += int(seconds)

    return timedelta(seconds=total_seconds)

def average_duration_per_channel():
    try:
        # Query to fetch channelTitle and duration from videos table
        cursor.execute('''
            SELECT channelTitle, duration
            FROM videos
        ''')

        # Fetch all rows
        rows = cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['channelTitle', 'duration'])

        # Convert duration string to timedelta
        df['duration'] = df['duration'].apply(parse_duration)

        # Calculate average duration per channelTitle
        avg_durations = df.groupby('channelTitle')['duration'].mean()

        return avg_durations

    except Exception as e:
        print(f"Error fetching average durations: {e}")
        return None

def nine():
    avg_durations = average_duration_per_channel()
    st.write(avg_durations)

def ten():
    cursor.execute('''select title , channelTitle , commentCount from videos where commentCount is not null order by commentCount desc;''')
    projectA.commit()
    q10=cursor.fetchall()
    st.write(pd.DataFrame(q10, columns=['Video Title','Channel Name','Comments Count']))

# Streamlit interface
st.set_page_config(layout="wide")
st.title("YOUTUBE DATA HARVESTING")
st.caption("Get Datas from the selected channel")

channel_id = st.text_input("Enter Channel ID(s) [Separate by comma( , )]")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

# Button to fetch and store data
if st.button("Fetch and Save Data"):
    for channel in channels:
        query = {'channel_Id': channel}
        document = collection.find_one(query)
        if document:
            st.write("Channel Details already exists")
        else:
            output = channel_Details(channel)
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
         'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
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
elif query == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    nine()
elif query=='Videos with highest number of comments':
    ten()
