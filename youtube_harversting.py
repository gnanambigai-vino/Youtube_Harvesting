import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
import pymongo
import pandas as pd
import mysql.connector as sql

# SETTING PAGE CONFIGURATIONS
icon = Image.open(r"C:\Users\GVJai\Desktop\Project\YouTube\YouTube_Logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded"
                   )

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract","Transform","View"], 
                           icons=["house-door-fill","tools","airplane-fill","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "15px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "15px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
    
# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb+srv://gnanambigai:gnanambigai@cluster0.g6y23xg.mongodb.net/")
db = client.youtube_data

from googleapiclient.discovery import build

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyD92Hk_YJQA1YgOZG2H51O_p1L5-e9SEmU"
youtube = build('youtube','v3',developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(ch_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= ch_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = response['items'][i]['id'],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country'),
                    video_details = get_video_details(get_channel_videos(ch_id))
                    )
        ch_data.append(data)
    return ch_data 

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption'],
                                Comment_details=get_comments_details(video['id'])
                               )
            video_stats.append(video_details)
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

def main_function(ch_id):
  ch_id = ch_id
  ch_details = get_channel_details(ch_id)

  return {
          "Channel_details":ch_details
        }

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_detail.find():
        ch_name.append(i["Channel_details"][0]["Channel_name"])
    return ch_name

# CONNECTING WITH MYSQL DATABASE
mysql_connection = sql.connect(host="localhost",
                user="jai",
                password="jai123",
                database= "youtube_db",
                auth_plugin='mysql_native_password',
                charset='utf8mb4'
                )
mysql_cursor = mysql_connection.cursor()

def MongoDBtoMySQL(ch_name):
   
    # Collection Name
    collections = db.channel_detail
    mongodb_data = collections.find({"Channel_details.Channel_name" : ch_name})
    
 
    # Define a MySQL INSERT statement
    mysql_Channel_insert_query = "INSERT INTO channel_details (Channel_id, Channel_name,Playlist_id,Subscribers,Views,Total_videos,Description,Country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_Video_insert_query = "INSERT INTO video_details (Video_id, Title,Thumbnail,Description,Published_date,Duration,Views,Likes,Comments,Favorite_count,Definition,Channel_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_Comment_insert_query = "INSERT INTO comment_details (Comment_id, Comment_text,Comment_author,Comment_posted_date,Like_count,Reply_count,Video_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    # Loop through MongoDB data and update MySQL
    for document in mongodb_data:
        # Check if the data already exists in MySQL based on some criteria (e.g., Channel_id)
        mysql_cursor.execute("SELECT COUNT(*) FROM channel_details WHERE Channel_id = %s", (document["Channel_details"][0]["Channel_id"],))
        count = mysql_cursor.fetchone()[0]

        if count == 0:
            data_to_insert = (document["Channel_details"][0]["Channel_id"], document["Channel_details"][0]["Channel_name"],document["Channel_details"][0]["Playlist_id"],document["Channel_details"][0]["Subscribers"],document["Channel_details"][0]["Views"],document["Channel_details"][0]["Total_videos"],document["Channel_details"][0]["Description"],document["Channel_details"][0]["Country"])  # Replace with actual field names
            mysql_cursor.execute(mysql_Channel_insert_query, data_to_insert)
            for video in document["Channel_details"][0]["video_details"]:
                data_to_insert_video=(video["Video_id"],video["Title"],video["Thumbnail"],video["Description"],video["Published_date"],video["Duration"],video["Views"],video["Likes"],video["Comments"],video["Favorite_count"],video["Definition"],document["Channel_details"][0]["Channel_id"])
                mysql_cursor.execute(mysql_Video_insert_query, data_to_insert_video)
                if(len(video["Comment_details"])>0):
                    for comment in video["Comment_details"]:
                        data_to_insert_comment=(comment["Comment_id"],comment["Comment_text"],comment["Comment_author"],comment["Comment_posted_date"],comment["Like_count"],comment["Reply_count"],video["Video_id"])
                        mysql_cursor.execute(mysql_Comment_insert_query, data_to_insert_comment)
            
    # Commit the changes to MySQL and close the connection
    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

# HOME PAGE
if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")
 
# EXTRACT PAGE
if selected == "Extract":
    st.markdown("#    ")
    st.write("### Enter YouTube Channel_ID below :")
    col1, col2 = st.columns(2)
    ch_id = col1.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
    col2.markdown("#   ")
    if col2.button("Extract Data"):
        ch_details = main_function(ch_id)
        st.write(f'#### Extracted data from :green["{ch_details["Channel_details"][0]["Channel_name"]}"] channel')
        st.table(ch_details)

    if col1.button("Upload to MongoDB"):
        with st.spinner('Please Wait for it...'):
            ch_details = main_function(ch_id)
            collections = db.channel_detail
            collections.insert_one(ch_details)
            st.success("Upload to MogoDB successful !!")

# TRANSFORM PAGE
if selected =="Transform":    
    st.markdown("#   ")
    st.markdown("### Select a channel to begin Transformation to SQL")
    
    ch_names = channel_names()
    user_inp = st.selectbox("Select channel",options= ch_names)
    
    if st.button("Submit"):
        try:
            MongoDBtoMySQL(user_inp)
            st.success("Transformation to MySQL Successful !!")
        except:
            st.error("Channel details already transformed !!")
       
# VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mysql_cursor.execute("""select vd.Title,cd.Channel_name from video_details vd 
                             join channel_details cd on vd.Channel_id=cd.Channel_id""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mysql_cursor.execute("""select cd.Channel_name, count(vd.Video_id) as VideoCount from channel_details cd 
                                join video_details vd on cd.Channel_id=vd.Channel_id
                                group by cd.Channel_name order by VideoCount desc limit 1;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mysql_cursor.execute("""select vd.Title, cd.Channel_name, vd.Views from video_details vd
                                join channel_details cd on vd.Channel_id=cd.Channel_id
                                order by vd.Views desc limit 10;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mysql_cursor.execute("""select vd.Title, count(*) as CommentCount from comment_details cm 
                                join video_details vd on cm.Video_id=vd.Video_id
                                group by vd.Title;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mysql_cursor.execute("""select vd.Title, cd.Channel_name, vd.Likes from video_details vd 
                                join channel_details cd on vd.Channel_id=cd.Channel_id 
                                order by vd.Likes desc limit 1;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        mysql_cursor.execute("""select vd.Title, sum(vd.Likes) as TotalLikes from video_details vd 
                                group by vd.Title;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mysql_cursor.execute("""select cd.Channel_name, sum(vd.Views) as TotalViews from channel_details cd
                                join video_details vd on cd.Channel_id=vd.Channel_id
                                group by cd.Channel_name;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mysql_cursor.execute("""select distinct cd.Channel_name
                                from channel_details cd join video_details vd on  cd.Channel_id = vd.Channel_id
                                where year(str_to_date(vd.Published_date, '%Y-%m-%dT%H:%i:%sZ')) = 2022;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mysql_cursor.execute("""select cd.Channel_name,avg(substring_index(substring_index(vd.Duration, 'T', -1), 'M', 1) * 60 +
                                substring_index(substring_index(vd.Duration, 'M', -1), 'S', 1)) as average_duration_seconds from 
                                channel_details cd join video_details vd on cd.Channel_id = vd.Channel_id group by cd.Channel_name;""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mysql_cursor.execute("""select vd.Title as Video_Title, cd.Channel_name as Channel_Name, cd.Channel_id as Channel_ID,
                                vd.Comments as Number_of_Comments from video_details vd
                                join channel_details cd on vd.Channel_id = cd.Channel_id
                                where vd.Comments = (select max(Comments) from video_details);""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        df.index=df.index+1
        st.write(df)
        