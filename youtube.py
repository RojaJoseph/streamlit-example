from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
import  mysql.connector

api_key='AIzaSyDcZwzjlr4kxRdMt4FxtgFXapYvW9BrjK8'

api_service_name="youtube"
api_version="v3"
channel_id=['UC2UXDak6o7rBm23k3Vv5dww']    
youtube=build(api_service_name,api_version,developerKey=api_key)


def get_channel_info(channel_id):
    request=youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id     
    )
    response =request.execute()

    
    for i in response['items']: 
        data=dict(channel_Name =i["snippet"]["title"],
                            channel_Id=i["id"],
                            subscribers=i["statistics"]["subscriberCount"],
                            views=i["statistics"]["viewCount"],
                            Total_videos=i["statistics"]["videoCount"],
                            channel_des=i["snippet"]["description"],
                            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data       
    
def get_video_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                  part = 'contentDetails').execute()
    playlist_Id =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True: 

       response1 = youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
       for i in range(len(response1['items'])):                  
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token=response1.get('nextPageToken')
                            

       if next_page_token is None:
         break 
    return video_ids


def get_video_info(VIDEO_IDS):
    video_Data=[]
    for video_id in VIDEO_IDS:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            Data = dict(channel_Name = item['snippet']['channelTitle'],
                        channel_Id =item['snippet']['channelId'],
                        video_Id = item['id'],
                        Title=item['snippet']['title'],
                        Tags = item.get('tags'),
                        thumbnail =item['snippet']['thumbnails']['default']['url'],
                        description = item.get('description'),
                        Published_date =item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item.get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item.get('commentCount'),
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_status=item['contentDetails'].get['caption']
                        )
            
            video_Data.append(Data)
    return video_Data


def get_comment_info(VIDEO_IDS):
    Comment_data=[]
    try:                             
        for video_id in VIDEO_IDS:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            
            response=request.execute()    

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                
                Comment_data.append(data)
                            
    except:
        pass
    return Comment_data


def get_playlist_details(channel_id):
        
        All_data=[]

        next_page_token=None
        while True:            
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token

                )
                response=request.execute()

                for item in response['items']:
                        data=dict(playlist_Id=item['id'],
                                playlist_title=item['snippet']['title'],
                                CHANNEL_ID=item['snippet']['channelId'],
                                channel_Name=item['snippet']['channelTitle'],
                                publishedAt=item['snippet']['publishedAt'],
                                Video_count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                break  
        return All_data  

Client = pymongo.MongoClient("mongodb://localhost:27017")
db=Client["YOUTUBE_DATA"]

def channel_views(channel_id):        
            
    ch_details=get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    pl_details= get_playlist_details(channel_id)

    coll1=db["channel_views"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
    "video_information":vi_details,"comment_information":com_details})

    
    return "upload complted successfully"
                                                                    

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "Rojamary2023"
    )

print(mydb)
mycursor = mydb.cursor()

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Rojamary2023"
)

mycursor = mydb.cursor()

mycursor.execute ("CREATE DATABASE if not exists youtube_data")

print("Database created successfully.")

def channel_table():

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Rojamary2023",
    database='youtube_data'
    )
    mycursor = mydb.cursor()
    
    create_query='''create table channels1(channel_Name VARCHAR(80),
                                        channel_Id varchar(80),
                                        subscribers int,
                                        views int,
                                        Total_videos int,
                                        channel_des TEXT,
                                        Playlist_Id int
                                        )'''
    mycursor.execute(create_query)
      
    ch_list=[]
    db=Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list) 


    for Index,row in df.iterrows():
        insert_query='''insert into channels1(channel_Name,
                                                  channel_Id,
                                                  subscribers,
                                                  views,
                                                  Total_videos,
                                                  channel_des,
                                                  Playlist_Id)
                                    
                                                  values (%s, %s, %s, %s, %s, %s, %s)'''
                                    
        values=(row['channel_Name'],
                    row['channel_Id'],
                    row['subscribers'],
                    row['views'],
                    row['Total_videos'],
                    row['channel_des'],
                    row['Playlist_Id'])
               
        mycursor.execute(insert_query,values)
        mydb.commit()        
                   
                        
def playlist_table():                 
        mydb=mysql.connector.connect(
                host="localhost",
                user="root",
                password="Rojamary2023",
                database='youtube_data')
        mycursor=mydb.cursor()

        create_query='''CREATE TABLE playlists(playlist_Id varchar(100) ,
                                                playlist_title varchar(100),
                                                CHANNEL_ID varchar(100),
                                                channel_Name varchar(100),
                                                publishedAt timestamp,
                                                Video_count int
                                                )'''
        mycursor.execute(create_query)
        mydb.commit()

        pl_list=[]
        db=Client["YOUTUBE_DATA"]
        coll1=db["channel_views"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                    pl_list.append(pl_data["playlist_information"][i])
        df1=pd.DataFrame(pl_list)        


        for index,row in df1.iterrows():
                insert_query='''insert into playlists(playlist_Id,
                                                        playlist_title,
                                                        CHANNEL_ID,
                                                        channel_Name,
                                                        PublishedAt,
                                                        Video_count)

                                                        values (%s, %s, %s, %s, %s, %s)'''
                        
                values=(row['playlist_Id'],
                        row['playlist_title'],
                        row['CHANNEL_ID'],
                        row['channel_Name'],
                        row['publishedAt'],
                        row['Video_count'],)
                
                mycursor.execute(insert_query,values)
                mydb.commit()
                
def video_table():
    mydb=mysql.connector.connect(
                host="localhost",
                user="root",
                password="Rojamary2023",
                database='youtube_data')
    mycursor=mydb.cursor()

    create_query='''create table videos(channel_Name varchar(80),
                                          channel_Id varchar(80),
                                          video_Id varchar(80),
                                          Title varchar(100),
                                          Tags text,
                                          thumbnail varchar(80),
                                          description text,
                                          Published_date timestamp,
                                          Duration time,
                                          Views int,
                                          Likes int,
                                          Comments text,
                                          Favorite_count int,
                                          Definition varchar(100),
                                          Caption_status varchar(50)
                                            )'''
    mycursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)   
        
    for index,row in df2.iterrows():      
        insert_query='''insert into videos(channel_Name,
                                            channel_Id,
                                            video_Id,
                                            Title,
                                            Tags,
                                            thumbnail,
                                            description,
                                            Published_date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_count,
                                            Definition,
                                            Caption_status) 

                                            Values=(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''


        Values=(row['channel_Name'],
                row['channel_Id'],
                row['video_Id'],
                row['Title'],
                row['Tags'],
                row['thumbnail'],
                row['description'],
                row['Published_date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption_status'])
            
            
        mycursor.execute(insert_query,Values)
        mydb.commit()
                          
def comments_table():    
    mydb=mysql.connector.connect(
                host="localhost",
                user="root",
                password="Rojamary2023",
                database='youtube_data')
    mycursor=mydb.cursor()

    create_query = '''create table comments(Comment_Id VARCHAR(100),
                                            Video_Id VARCHAR(100),
                                            Comment_Text text,
                                            Comment_Author varchar(150),
                                            Comment_Published timestamp )'''

    mycursor.execute(create_query)
    mydb.commit()

    co_list=[]
    db=Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for co_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(co_data["comment_information"])):
            co_list.append(co_data["comment_information"][i])
    df3 = pd.DataFrame(co_list)   


    for index,row in df3.iterrows(): 
        insert_query = '''INSERT INTO comments(Comment_Id,
                                               Video_id ,
                                               Comment_Text ,
                                               Comment_Published,
                                               Comment_Author )


                                                Values( %s,%s,%s,%s,%s)'''
                
        Values=(row['Comment_Id'],row['Video_Id'],row['Comment_Text'],row['Comment_Published'],row['Comment_Author'])
        mycursor.execute(insert_query,Values)
        mydb.commit()

def tables():
    channel_table()
    playlist_table()
    video_table()
    comments_table()
    
    return "tables created successfully"
    Tables = tables()

def show_channels_table():
            
    ch_list = []
    db = Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for ch_data in coll1.find({},{"_id":1,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)    
    return df

def show_playlist_table():
        
    pl_list = []
    db = Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)    
    return df1

def show_videos_table(): 
    vi_list = []
    db = Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for vi_data in coll1.find({},{"_id":1,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():    
    co_list=[]
    db = Client["YOUTUBE_DATA"]
    coll1=db["channel_views"]
    for co_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(co_data["comment_information"])):
            co_list.append(co_data["comment_information"][i])
    df3 = st.dataframe(co_list)   
    return df3

with st.sidebar:
    st.title("pink[youtube Data Harvesting and Warehousing]")
    st.header("Learning Part")
    st.caption("Python Script")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

Channel_id = st.text_input("Enter the channel ID")

if st.button("Import and Export"):
    ch_ids = []
    ch=Client["YOUTUBE_DATA"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

if st.button("SQl"):
    Table = tables()


show_table = st.radio,("CHANNELS","PLAYLIST","VIDEOS","COMMENTS")
                                    
if show_table =="CHANNELS":
    show_channels_table()

elif show_table =="PLAYLIST":
    show_playlist_table()

elif show_table =="COMMENTS":
    show_comments_table()
    
elif show_table =="videos":
    show_videos_table()               
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Rojamary2023",
    database="YOUTUBE_DATA"
)

mycursor = mydb.cursor()

question = st.selectbox("Select your question",("1.All the videos and the channel name",
                                                "2.channels with most number of videos",
                                                "3.10 most viewd videos",
                                                "4.commends in each videos",
                                                "5.Videos with highest likes",
                                                "6.likes of all videos",
                                                "7.views of each channels",
                                                "8.vieos published in the year of 2022",
                                                "9.average duration of all videos in each channel",
                                                "10.videos with highest number of comments"))   
                                                
                                                  
