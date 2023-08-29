import os
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mysql.connector
from googleapiclient.discovery import build
import pymongo
import json
from PIL import Image


image = Image.open(r"C:\Users\user\Desktop\yt\Youtube-data-warehousing\image.jpg")
st.image(image, caption='youtube')

st.title('Youtube Data warehousing')
col1,col2=st.columns(2)
col1.write("Access the side bar > to perform tasks")


def get_channel_data(channel_id,api_key):
    youtube = build('youtube', 'v3', developerKey=api_key)
    # Retrieve channel information
    channels_response = youtube.channels().list(
        part='snippet,statistics',
        id=channel_id
    ).execute()

    if 'items' not in channels_response or len(channels_response['items']) == 0:
        print("Channel not found.")
        return None

    channel = channels_response['items'][0]
    channel_name = channel['snippet']['title']
    subscriber_count = int(channel['statistics']['subscriberCount'])
    channel_views = int(channel['statistics']['viewCount'])
    channel_description = channel['snippet']['description']

    # Retrieve playlists
    playlists = []
    next_page_token = None
    while True:
        playlists_response = youtube.playlists().list(
            part='snippet',
            channelId=channel_id,
            maxResults=10,  # Increase or decrease as needed
            pageToken=next_page_token
        ).execute()

        if 'items' in playlists_response:
            playlists.extend(playlists_response['items'])

        next_page_token = playlists_response.get('nextPageToken')
        if not next_page_token:
            break

    if len(playlists) == 0:
        print("No playlists found.")
        return None

    videos = {}
    for playlist in playlists:
        playlist_id = playlist['id']
        playlist_name = playlist['snippet']['title']

        # Retrieve videos for the playlist
        videos_response = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=10  # Increase or decrease as needed
        ).execute()

        for video_item in videos_response['items']:
            video_id = video_item['snippet']['resourceId']['videoId']

            # Retrieve video details
            video_details_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            ).execute()

            if 'items' not in video_details_response or len(video_details_response['items']) == 0:
                print(f"Video not found: {video_id}")
                continue

            video = video_details_response['items'][0]
            video_name = video['snippet']['title']
            video_description = video['snippet']['description']
            tags = video['snippet']['tags'] if 'tags' in video['snippet'] else []
            published_at = video['snippet']['publishedAt']
            view_count = int(video['statistics']['viewCount'])
            like_count = int(video['statistics']['likeCount'])
            dislike_count = int(video['statistics'].get('dislikeCount', 0))  # Handle missing 'dislikeCount' key
            favorite_count = int(video['statistics']['favoriteCount'])
            comment_count = int(video['statistics']['commentCount'])
            duration = video['contentDetails']['duration']
            thumbnail = video['snippet']['thumbnails']['default']['url']
            caption_status = video['snippet']['localized'].get('isCaptionAvailable',
                                                               False)  # Handle missing 'isCaptionAvailable' key

            # Retrieve video comments
            comments_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=10
            ).execute()

            comments = {}
            for comment_item in comments_response['items']:
                comment_id = comment_item['snippet']['topLevelComment']['id']
                comment_text = comment_item['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_author = comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_published_at = comment_item['snippet']['topLevelComment']['snippet']['publishedAt']

                comment_data = {
                    'comment_id': comment_id,
                    'comment_text': comment_text,
                    'comment_author': comment_author,
                    'comment_published_at': comment_published_at
                }
                comments[comment_id] = comment_data

            video_data = {
                'video_id': video_id,
                'video_name': video_name,
                'video_description': video_description,
                'tags': tags,
                'published_at': published_at,
                'view_count': view_count,
                'like_count': like_count,
                'dislike_count': dislike_count,
                'favorite_count': favorite_count,
                'comment_count': comment_count,
                'duration': duration,
                'thumbnail': thumbnail,
                'caption_status': caption_status,
                'comments': comments,
                'Playlist_name': playlist_name,
                'playlist_id': playlist_id
            }
            videos[video_id] = video_data

    # Return channel data and video information
    channel_data = {
        'channel_name': {
            'channel_name': channel_name,
            'channel_id': channel_id,
            'subscription_count': subscriber_count,
            'channel_views': channel_views,
            'channel_description': channel_description,
            'playlists': []
        },
        **videos
    }

    for playlist in playlists:
        playlist_id = playlist['id']
        playlist_name = playlist['snippet']['title']
        channel_data['channel_name']['playlists'].append({
            'playlist_id': playlist_id,
            'playlist_name': playlist_name
        })

    def export_to_mongodb():
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017")
        db = client["Youtube_Data_Warehousing"]
        collection = db["Youtube_data"]

        # insert data into MangoDB
        if channel_data:
            # Insert the channel data into MongoDB
            collection.insert_one(channel_data)
            st.write("Data inserted into MongoDB.")
        else:
            st.write("Error retrieving channel data.")
    export_to_mongodb()

    return channel_data



def migrate_to_mysql(name):
    #retrieve data from database
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["Youtube_Data_Warehousing"]
    collection = db["Youtube_data"]
    data = collection.find_one({'channel_name.channel_name':name})


    # Table channel
    # creating dict to retrieve data from "data" using attributes and appending to respective keys as list of values.
    table_channel = {'channel_id': [], 'channel_name': [], 'channel_views': [], 'channel_description': []}
    table_channel['channel_id'].append(data['channel_name']['channel_id'])
    table_channel['channel_name'].append(data['channel_name']['channel_name'])
    table_channel['channel_views'].append(data['channel_name']['channel_views'])
    table_channel['channel_description'].append(data['channel_name']['channel_description'])
    table_channel_1 = pd.DataFrame.from_dict(table_channel)

    #Table playlist
    table_playlist = {'playlist_id': [], 'channel_id': [], 'playlist_name': []}
    for i in range(len(data['channel_name']['playlists'])):
        table_playlist['playlist_id'].append(data['channel_name']['playlists'][i]['playlist_id'])
    for i in range(len(data['channel_name']['playlists'])):
        table_playlist['channel_id'].append(data['channel_name']['channel_id'])
    for i in range(len(data['channel_name']['playlists'])):
        table_playlist['playlist_name'].append(data['channel_name']['playlists'][i]['playlist_name'])
    table_playlist_1 = pd.DataFrame.from_dict(table_playlist)

    #Table comment
    table_comments = {'comment_id': [], 'video_id': [], 'comments_text': [], 'comment_author': [],
                      'comment_published_date': []}
    dict_keys = list(data.keys())
    comment_key = dict_keys[2:]
    for i in comment_key:
        for j in data[i]['comments']:
            for k in data[i]['comments'][j]:
                table_comments['comment_id'].append(data[i]['comments'][j]['comment_id'])
                table_comments['comments_text'].append(data[i]['comments'][j]['comment_text'])
                table_comments['comment_published_date'].append(data[i]['comments'][j]['comment_published_at'])
                table_comments['comment_author'].append(data[i]['comments'][j]['comment_author'])
                table_comments['video_id'].append(i)
    table_comment_1 = pd.DataFrame.from_dict(table_comments)

    #Table video
    table_video = {'channel_id': [], 'video_id': [], 'playlist_id': [], 'video_name': [], 'video_description': [], 'published_date': [],
                   'view_count': [],
                   'like_count': [], 'dislike_count': [], 'favorite_count': [], 'comment_count': [],
                   'duration': [], 'thumbnail': [], 'caption_status': []}

    for i in comment_key:
        table_video['channel_id'].append(data['channel_name']['channel_id'])
        table_video['video_id'].append(data[i]['video_id'])
        table_video['playlist_id'].append(data[i]['playlist_id'])
        table_video['video_name'].append(data[i]['video_name'])
        table_video['video_description'].append(data[i]['video_description'])
        table_video['published_date'].append(data[i]['published_at'])
        table_video['view_count'].append(data[i]['view_count'])
        table_video['like_count'].append(data[i]['like_count'])
        table_video['dislike_count'].append(data[i]['dislike_count'])
        table_video['favorite_count'].append(data[i]['favorite_count'])
        table_video['comment_count'].append(data[i]['comment_count'])
        table_video['duration'].append(data[i]['duration'])
        table_video['thumbnail'].append(data[i]['thumbnail'])
        table_video['caption_status'].append(data[i]['caption_status'])

    table_video_1 = pd.DataFrame.from_dict(table_video)

    # connecting to mysql database
    engine = create_engine('mysql+mysqlconnector://root:Bharatkori#1998@127.0.0.1:3306/youtube_data_warehouse')
    config = {
        'user': 'root',
        'password': '18BEme035$',
        'host': 'localhost',
        'database': 'yt_data_warehouse',
        'raise_on_warnings': True
    }



# Connect to the database
    cnx = mysql.connector.connect(**config)

    # Check if the connection is successful
    if cnx.is_connected():
        print("Connection to MySQL database established.")
    else:
        print("Connection to MySQL database failed.")

    # pushing structured data into mysql
    table_playlist_1.to_sql(name='table_playlist', con=engine, if_exists='append', index=False)
    table_channel_1.to_sql(name='table_channel', con=engine, if_exists='append', index=False)
    table_comment_1.to_sql(name='table_comment', con=engine, if_exists='append', index=False)
    table_video_1.to_sql(name='table_video', con=engine, if_exists='append', index=False)

    st.write('data moved to Mysql')

    return "data migrated successfully"

# Chanel analysis
def data_analysis():
    pass



# Sidebar selectbox
add_selectbox = st.sidebar.selectbox(
    "Tasks",
    ("Scrape data","channels present","migrate to MySQL","data_analysis")
)
# Perform action based on selected option

if add_selectbox == "Scrape data":
    api_key = st.text_input('enter your youtube api key')
    youtube_ID = st.text_input("enter youtube channel id")
    if st.button("Scrape"):
        channel_id = f"{youtube_ID}"
        channel_data = get_channel_data(channel_id,api_key)

        #if channel_data:
        #k = json.dumps(channel_data, indent=4)
        #st.json(k)

elif add_selectbox == "migrate to MySQL":
    name = st.text_input("enter correct channel name")
    if st.button("Migrate"):
        migrate_to_mysql(name)

elif add_selectbox == "channels present":
    if st.button("show"):
        client = pymongo.MongoClient("mongodb://localhost:27017")
        db = client["Youtube_Data_Warehousing"]
        collection = db["Youtube_data"]
        names = collection.find({}, {"channel_name.channel_name": 1 })
        for name in names:
            st.write(name['channel_name']['channel_name'])

elif add_selectbox == "data_analysis":
    engine = create_engine('mysql+mysqlconnector://root:18BEme035$@127.0.0.1:3306/yt_data_warehouse')
    # establishing connection to mysql database
    config = {
        'user': 'root',
        'password': '18BEme035$',
        'host': 'localhost',
        'database': 'youtube_data_warehouse',
        'raise_on_warnings': True
    }
    # Connect to the database
    cnx = mysql.connector.connect(**config)
    if st.button('What are the names of all the videos and their corresponding channels?',key='button1'):
        query = f'''
        SELECT c.channel_name, v.video_name 
        from table_channel c join table_video v on c.channel_id = v.channel_id
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)
    if st.button('What is the total number of likes and dislikes for each video, and what are their corresponding video names?',key='button2'):
        query = f'''
        SELECT c.channel_name, COUNT(v.video_id) AS video_count
        FROM table_channel c
        JOIN table_video v ON c.channel_id = v.channel_id
        GROUP BY c.channel_id, c.channel_name
        ORDER BY video_count DESC;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)
    if st.button('How many comments were made on each video, and what are their corresponding video names?',key='button3'):
        query = "SELECT video_name,comment_count FROM table_video;"
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('What are the top 10 most viewed videos and their respective channels?',key='button4'):
        query = f'''
        SELECT  c.channel_name ,v.video_name,v.view_count
        from table_video v 
        join table_channel c on c.channel_id = v.channel_id
        ORDER BY v.view_count DESC
        LIMIT 10;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('Which videos have the highest number of likes, and what are their corresponding channel names?',key='button5'):
        query = f'''
        select c.channel_name, v.video_name, v.like_count
        from table_video v
        join table_channel c on c.channel_id = v.channel_id
        ORDER BY v.like_count DESC
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('What is the total number of likes and dislikes for each video, and what are their corresponding video names?',key='button6'):
        query = f'''
        select video_name, like_count,dislike_count
        from table_video;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('What is the total number of views for each channel, and what are their corresponding channel names?',key='button7'):
        query = f'''
        SELECT c.channel_name, SUM(v.view_count) AS total_view_count
        FROM table_channel c
        JOIN table_video v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('What are the names of all the channels that have published videos in the year 2022?',key='button8'):
        query = f'''
        SELECT DISTINCT c.channel_name
        FROM table_channel c
        JOIN table_video v ON c.channel_id = v.channel_id
        WHERE YEAR(STR_TO_DATE(v.published_date, '%Y-%m-%dT%H:%i:%sZ')) = 2022;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)

    if st.button('Which videos have the highest number of comments, and what are their corresponding channel names?',key='button9'):
        query = f'''
        SELECT c.channel_name, v.video_name, v.comment_count AS max_comment_count
        FROM table_channel c
        JOIN table_video v ON c.channel_id = v.channel_id
        ORDER BY max_comment_count DESC;
        '''
        x = pd.read_sql(query, cnx)
        st.dataframe(x)
