from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#CONNECTING TO GOOGLE API

api_key = "AIzaSyBPKV1QryoZvJjLmz5f1EeTG0_D4AiwHpo"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey = api_key)

st.title('Youtube Data Harvesting and Warehousing')
channel_id = st.text_input('Enter Channel Id')

if st.button("Click to verify channel details availability"):
    channel_list = []
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    # Assuming you have channel_id defined somewhere before this point
    # If not, make sure to define it before checking in the channel_list

    for chlist in col1.find({}, {'_id': 0, 'channelinfo': 1}):
        channel_list.append(chlist['channelinfo']['channelId'])

    st.write(channel_list)
    # Now check if channel_id is in channel_list
    if channel_id in channel_list:
        st.success('Channel details already exist')
    else:
        st.success('Click upload button to get channel details')


def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        data = {
            'channelName': item['snippet']['title'],
            'channelId': item['contentDetails']['relatedPlaylists']['uploads'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'totalVideos': item['statistics']['videoCount'],
            'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],
            'description': item['snippet']['description']
        }

    return data


def get_video_id(youtube, channel_id):
    video_ids = []
    # get Uploads playlist id
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(part='snippet',
                                           playlistId=playlist_id,
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_details(youtube, video_ids):
    all_video_info = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)

        response = request.execute()

        for video in response['items']:
            data = dict(
                Channel_name=video['snippet']['channelTitle'],
                Channel_id=video['snippet']['channelId'],
                Video_id=video['id'],
                Title=video['snippet']['title'],
                Tags=video['snippet'].get('tags'),
                Thumbnail=video['snippet']['thumbnails']['default']['url'],
                Description=video['snippet']['description'],
                Published_date=video['snippet']['publishedAt'],
                Duration=video['contentDetails']['duration'],
                Views=video['statistics']['viewCount'],
                Likes=video['statistics'].get('likeCount'),
                Comments=video['statistics'].get('commentCount'),
                Favorite_count=video['statistics']['favoriteCount'],
                Definition=video['contentDetails']['definition'],
                Caption_status=video['contentDetails']['caption']
            )
            all_video_info.append(data)

    return all_video_info

def get_comments(youtube, video_ids):
    comment_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId= video_id,
                maxResults = 50
            )
            response = request.execute()
            comment_data = []
            for comments in response['items']:
                data = dict(
                        Comment_id = comments['id'],
                        Video_id = comments['snippet']['videoId'],
                        Comment = comments['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Author = comments['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Date_posted = comments['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Like_count = comments['snippet']['topLevelComment']['snippet']['likeCount'],
                        Reply_count = comments['snippet']['totalReplyCount']
                        )
                comment_data.append(data)
        except:
            break
    return comment_data

def get_playlists(youtube, channel_id):
    all_data = []
    request = youtube.playlists().list(
                part = 'snippet, contentDetails',
                channelId = channel_id,
                maxResults = 50
    )
    response = request.execute()

    for item in response['items']:
        data = dict(
                Playlist_Id = item['id'],
                Title = item['snippet']['title'],
                Channel_id = item['snippet']['channelId'],
                Channel_Name = item['snippet']['channelTitle'],
                Published_In = item['snippet']['publishedAt'],
                Video_count = item['contentDetails']['itemCount']

        )
        all_data.append(data)

    return all_data


def channel_data(channel_id):
    channel_details = get_channel_details(youtube, channel_id)
    video_ids = get_video_id(youtube, channel_id)
    video_details = get_video_details(youtube, video_ids)
    comment_details = get_comments(youtube, video_ids)
    playlist_details = get_playlists(youtube, channel_id)

    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    col1.insert_one({'channelinfo': channel_details,
                     'playlistinfo': playlist_details,
                     'videoinfo': video_details,
                     'commentinfo': comment_details})

    return st.success('Upload completed')

if st.button('upload'):
    try:
        channel_data(channel_id)
        st.success('channel details uploaded')
    except:
        st.write(":red[Enter correct channel id]")

client = pymongo.MongoClient("mongodb://localhost:27017")


def channelTable():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="15071965",
                            database="YoutubeData",
                            port="5432")

    cursor = mydb.cursor()

    drop_query = """drop table if exists channels"""
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """create table if not exists channels(channelName varchar(100),
                                                            channelId varchar(100) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            totalVideos int,
                                                            playlistId varchar(100),
                                                            description text)"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print('Table created')

    # Getting data from Mongodb and convert into dataframe
    channel_list = []
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    for channel_data in col1.find({}, {"_id": 0, "channelinfo": 1}):
        channel_list.append(channel_data['channelinfo'])
    df = pd.DataFrame(channel_list)

    # insert rows in sql database
    for index, row in df.iterrows():
        insert_query = """insert into channels(channelName,
                                            channelId,
                                            subscribers,
                                            views,
                                            totalVideos,
                                            playlistId,
                                            description)

                                            values(%s, %s, %s, %s, %s, %s, %s)"""

        values = (row['channelName'],
                  row['channelId'],
                  row['subscribers'],
                  row['views'],
                  row['totalVideos'],
                  row['playlistId'],
                  row['description'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print(f"Error inserting row at index {index}: {e}")
            mydb.rollback()  # Rollback the transaction in case of an error


def videoTable():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="15071965",
                            database="YoutubeData",
                            port="5432")

    cursor = mydb.cursor()

    drop_query = """drop table if exists videos"""
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """create table if not exists videos(Channel_name varchar(100),
                                                                Channel_id varchar(100),
                                                                Video_id varchar(100) primary key,
                                                                Title varchar(100),
                                                                Tags text,
                                                                Thumbnail varchar(255),
                                                                Description text,
                                                                Published_date timestamp,
                                                                Duration interval,
                                                                Views bigint,
                                                                Likes bigint,
                                                                Comments int,
                                                                Favorite_count int,
                                                                Definition varchar(20),
                                                                Caption_status varchar(50))"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Video list created")

    # Getting data from Mongodb and convert into dataframe
    videolist = []
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    for video_data in col1.find({}, {"_id": 0, "videoinfo": 1}):
        for i in range(len(video_data['videoinfo'])):
            videolist.append(video_data['videoinfo'][i])
    df2 = pd.DataFrame(videolist)

    # insert rows in sql database
    for index, row in df2.iterrows():
        insert_query = """insert into videos(Channel_name,
                                            Channel_id,
                                            Video_id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_count,
                                            Definition,
                                            Caption_status
                                            )

                                            values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        values = (row['Channel_name'],
                  row['Channel_id'],
                  row['Video_id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_count'],
                  row['Definition'],
                  row['Caption_status'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print(f"Error inserting row at index {index}: {e}")
            mydb.rollback()  # Rollback the transaction in case of an error


def playlistTable():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="15071965",
                            database="YoutubeData",
                            port="5432")

    cursor = mydb.cursor()

    drop_query = """drop table if exists playlists"""
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                Title varchar(100),
                                                                Channel_id varchar(100),
                                                                Channel_Name varchar(100),
                                                                Published_In timestamp,
                                                                Video_count int)"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Playlist created")
    # Getting data from Mongodb and convert into dataframe
    playlist = []
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    for playlist_data in col1.find({}, {"_id": 0, "playlistinfo": 1}):
        for i in range(len(playlist_data['playlistinfo'])):
            playlist.append(playlist_data['playlistinfo'][i])
    df1 = pd.DataFrame(playlist)

    # insert rows in sql database
    for index, row in df1.iterrows():
        insert_query = """insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_id,
                                            Channel_Name,
                                            Published_In,
                                            Video_count)

                                            values(%s, %s, %s, %s, %s, %s)"""

        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_id'],
                  row['Channel_Name'],
                  row['Published_In'],
                  row['Video_count'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print(f"Error inserting row at index {index}: {e}")
            mydb.rollback()  # Rollback the transaction in case of an error


def commentTable():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="15071965",
                            database="YoutubeData",
                            port="5432")

    cursor = mydb.cursor()

    drop_query = """drop table if exists comments"""
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """create table if not exists comments(Comment_id varchar(100) primary key,
                                                                Video_id varchar(100),
                                                                Comment text,
                                                                Author varchar(100),
                                                                Date_posted timestamp,
                                                                Like_count int,
                                                                Reply_count int)"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("comment list created")
    # Getting data from Mongodb and convert into dataframe
    comment = []
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    for comment_data in col1.find({}, {"_id": 0, "commentinfo": 1}):
        for i in range(len(comment_data['commentinfo'])):
            comment.append(comment_data['commentinfo'][i])
    df3 = pd.DataFrame(comment)

    # insert rows in sql database
    for index, row in df3.iterrows():
        insert_query = """insert into comments(Comment_id,
                                            Video_id,
                                            Comment,
                                            Author,
                                            Date_posted,
                                            Like_count,
                                            Reply_count)

                                            values(%s, %s, %s, %s, %s, %s, %s)"""

        values = (row['Comment_id'],
                  row['Video_id'],
                  row['Comment'],
                  row['Author'],
                  row['Date_posted'],
                  row['Like_count'],
                  row['Reply_count'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print(f"Error inserting row at index {index}: {e}")
            mydb.rollback()  # Rollback the transaction in case of an error

def migrate_to_sql():
    channelTable()
    videoTable()
    playlistTable()
    commentTable()

if st.button('Migrate Data to SQL'):
    migrate_to_sql()
    st.success('Data Migrated Successfully')

def table_channel():
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]

    channel_list = []
    for channel_data in col1.find({},{"_id":0,"channelinfo":1}):
        channel_list.append(channel_data['channelinfo'])
    df = st.dataframe(channel_list)
    return df

def table_playlist():
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]
    playlist = []
    for playlist_data in col1.find({},{"_id":0,"playlistinfo":1}):
        for i in range(len(playlist_data['playlistinfo'])):
            playlist.append(playlist_data['playlistinfo'][i])
    df1 = st.dataframe(playlist)
    return df1

def table_video():
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]
    videolist = []
    for video_data in col1.find({},{"_id":0,"videoinfo":1}):
        for i in range(len(video_data['videoinfo'])):
            videolist.append(video_data['videoinfo'][i])
    df2 = st.dataframe(videolist)
    return df2

def table_comment():
    db = client["Youtube_Data"]
    col1 = db["ChannelDetails"]
    comment = []
    for comment_data in col1.find({},{"_id":0,"commentinfo":1}):
        for i in range(len(comment_data['commentinfo'])):
            comment.append(comment_data['commentinfo'][i])
    df3 = st.dataframe(comment)
    return df3

table = st.radio('Select to table to view', ('Channel Table','Video Table','Playlist Table','Comments Table'))

if table == 'Channel Table':
    table_channel()
if table == 'Video Table':
    table_video()
if table == 'Playlist Table':
    table_playlist()
if table == 'Comments Table':
    table_comment()

mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "15071965",
                            database = "YoutubeData",
                            port = "5432")

cursor = mydb.cursor()

drop_query = """drop table if exists videos"""
cursor.execute(drop_query)
mydb.commit()

questions = st.selectbox('Questions', ('1. Names of all videos and their corresponding channels',
                                       '2. Channels with the most number of videos and the number of videos they have',
                                       '3. Top 10 most viewed videos and their respective channels',
                                       '4. Number of comments on each video and their corresponding video names',
                                       '5. Videos with the highest number of likes and their corresponding channel names',
                                       '6. Total number of likes for each video and their corresponding video names',
                                       '7. Total number of views for each channel and their corresponding channel names',
                                       '8. Names of channels that published videos in the year 2022',
                                       '9. Videos with the highest number of comments and their corresponding channel names'))


if questions == '1. Names of all videos and their corresponding channels':
    query1 = '''select title,channel_name from videos'''
    cursor.execute(query1)
    mydb.commit()
    s1 = cursor.fetchall()
    dfs1 = pd.DataFrame(s1,columns=['video', 'channel'])
    st.write(dfs1)

elif questions == '2. Channels with the most number of videos and the number of videos they have':
    query2 = '''select channelname, totalvideos from channels order by totalvideos desc;'''
    cursor.execute(query2)
    mydb.commit()
    s2 = cursor.fetchall()
    dfs2 = pd.DataFrame(s2,columns=['channel', 'No of Videos'])
    st.write(dfs2)

elif questions == '3. Top 10 most viewed videos and their respective channels':
    query3 = '''select channel_name, title, views from videos where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    s3 = cursor.fetchall()
    dfs3 = pd.DataFrame(s3, columns=['channel', 'video', 'views'])
    st.write(dfs3)

elif questions == '4. Number of comments on each video and their corresponding video names':
    query4 = '''select comments, title from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    s4 = cursor.fetchall()
    dfs4 = pd.DataFrame(s4, columns=['No of comments','videos'])
    st.write(dfs4)

elif questions == '5. Videos with the highest number of likes and their corresponding channel names':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    s5 = cursor.fetchall()
    dfs5 = pd.DataFrame(s5, columns=['video','channel', 'No of likes'])
    st.write(dfs5)

elif questions == '6. Total number of likes for each video and their corresponding video names':
    query6 = '''select title, likes from videos where likes is not null'''
    cursor.execute(query6)
    mydb.commit()
    s6 = cursor.fetchall()
    dfs6 = pd.DataFrame(s6, columns = ['title','likes'])
    st.write(dfs6)

elif questions == '7. Total number of views for each channel and their corresponding channel names':
    query7 = '''select channelname , views from channels where views is not null'''
    cursor.execute(query7)
    mydb.commit()
    s7 = cursor.fetchall()
    dfs7 = pd.DataFrame(s7, columns = ['channel','views'])
    st.write(dfs7)

elif questions == '8. Names of channels that published videos in the year 2022':
    query8 = '''select channel_name , title from videos where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    s8 = cursor.fetchall()
    dfs8 = pd.DataFrame(s8, columns = ['channel','videos published in 2022'])
    st.write(dfs8)

else:
    query9 = '''select channel_name, title, comments from videos where comments is not null order by comments desc'''
    cursor.execute(query9)
    mydb.commit()
    s9 = cursor.fetchall()
    dfs9 = pd.DataFrame(s9, columns = ['channel','video','total no of comments'])
    st.write(dfs9)