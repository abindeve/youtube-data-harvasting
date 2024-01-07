import streamlit as st
import mysql.connector
from pymongo import MongoClient
import os
import google.auth
import pandas as pd
from googleapiclient.discovery import build
import re
from googleapiclient.errors import HttpError
from streamlit_option_menu import option_menu
from datetime import datetime
import plotly.express as px
from youtubeAPI import get_channel_playlists_videos_and_comments
from database import create_table,connect_sql_databse,execute_mysql_query,save_to_mysql
from process import process_and_save_channel_data
def create_database_if_not_exists():
    connection = mysql.connector.connect(host="localhost", user="root", password="")
    cursor = connection.cursor()

    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS abin")
        print("Database created successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    cursor.close()
    connection.close()

# Function to save data to MongoDB
def save_to_mongodb(channel_data):
    host_name = "localhost"
    port = 27017
    client = MongoClient(host_name, port)
    db = client["youtube_data"]
    collection = db["channel_data"]
    collection.insert_one(channel_data)
    client.close()




mysql_connection_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "abin",
}

# MongoDB connection details
mongodb_connection_config = {
    "host": "localhost",
    "port": 27017,
}


def connect_mysql():
    return mysql.connector.connect(**mysql_connection_config)


def connect_mongodb():
    client = MongoClient(**mongodb_connection_config)
    return client




# Function to save playlist data to MySQL



def save_channel_to_mongodb(channel_data):
    host_name = "localhost"
    port = 27017
    client = MongoClient(host_name, port)
    db = client["youtube_data"]
    collection = db["channels"]
    collection.replace_one(
        {"channel_id": channel_data["channel_id"]}, channel_data, upsert=True
    )
    client.close()


def save_playlist_to_mongodb(playlist_data):
    host_name = "localhost"
    port = 27017
    client = MongoClient(host_name, port)
    db = client["youtube_data"]
    collection = db["playlists"]
    collection.replace_one(
        {"playlist_id": playlist_data["playlist_id"]}, playlist_data, upsert=True
    )
    client.close()


def save_video_to_mongodb(video_data):
    host_name = "localhost"
    port = 27017
    client = MongoClient(host_name, port)
    db = client["youtube_data"]
    collection = db["videos"]

    # Remove the 'comments' key from video_data before saving
    video_data_without_comments = video_data.copy()
    video_data_without_comments.pop("comments", None)

    collection.replace_one(
        {"video_id": video_data["video_id"]}, video_data_without_comments, upsert=True
    )
    client.close()


def save_comment_to_mongodb(comment_data):
    host_name = "localhost"
    port = 27017
    client = MongoClient(host_name, port)
    db = client["youtube_data"]
    collection = db["comments"]
    collection.replace_one(
        {"comment_id": comment_data["comment_id"]}, comment_data, upsert=True
    )
    client.close()


def main():
    # SETTING PAGE CONFIGURATIONS
    st.set_page_config(
        page_title="Youtube Data Harvesting and Warehousing | By Abin C Babu",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"About": """# This app is created by * Abin C Babu!*"""},
    )

    with st.sidebar:
        selected = option_menu(
            None,
            ["Home", "Queries"],
            icons=["house-door-fill",  "card-text"],
            default_index=0,
            orientation="vertical",
            styles={
                "nav-link": {
                    "font-size": "12px",
                    "text-align": "centre",
                    "margin": "0px",
                    "--hover-color": "#34999e",
                },
                "icon": {"font-size": "12px"},
                "container": {"max-width": "6000px"},
                "nav-link-selected": {"background-color": "#349e75"},
            },
        )
    st.title(":blue[YouTube Channel Data]")
    page = selected
    if page == "Home":
        api_key = st.text_input("Enter Your API Key",type='password')
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Submit"):
            if api_key and channel_id:
                channel_data = get_channel_playlists_videos_and_comments(
                    api_key, channel_id
                )
                save_channel_to_mongodb(channel_data["channel_data"])

                for playlist_data in channel_data["playlists"]:
                    save_playlist_to_mongodb(playlist_data)
                    for video_data in playlist_data["videos"]:
                        save_video_to_mongodb(video_data)
                        if video_data["comments"]:
                            for comment_data in video_data["comments"]:
                                save_comment_to_mongodb(comment_data)

                process_and_save_channel_data()
                st.success("Data saved successfully to MySQL and MongoDB")
                channel_info = channel_data["channel_data"]
                df = pd.DataFrame(data=[channel_info], columns=['channel_id','channel_name','channel_views','channel_description'])
                st.subheader("Channel Information:")
                st.write(df)
            else:
                st.warning("Please enter your channel ID.")

    elif page == "Queries":
        # st.title("SQL Query Output")
        selected_query = [
            "Choose an option",
            "1.What are the names of all the videos and their corresponding channels?",
            "2.Which channels have the most number of videos, and how many videos do they have?",
            "3.What are the top 10 most viewed videos and their respective channels?",
            "4.How many comments were made on each video, and what are their corresponding video names?",
            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
            "8.What are the names of all the channels that have published videos in the year 2022?",
            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10.Which videos have the highest number of comments, and what are their corresponding channel names?",
        ]

        # Create a select box with 30 options
        selected_option = st.selectbox("Choose an option", selected_query)
        if selected_option == selected_query[0]:
            st.write("Nothing to Display")
        elif selected_option == selected_query[1]:
            st.subheader("Names of all the videos and their corresponding channels")
            query = (
                "SELECT channel.channel_name, video.video_name "
                "FROM video INNER JOIN playlist ON video.playlist_id = playlist.playlist_id "
                "JOIN channel ON playlist.channel_id = channel.channel_id;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result)
                df.columns = ['Channel Name', 'Video Name']
                st.write(df)
        elif selected_option == selected_query[2]:
            st.subheader(
                "Which channels have the most number of videos, and how many videos do they have?"
            )
            query = (
                "SELECT channel.channel_name, count(video.video_name) AS video_count "
                "FROM video INNER JOIN playlist ON video.playlist_id = playlist.playlist_id "
                "JOIN channel ON playlist.channel_id = channel.channel_id GROUP BY channel.channel_name "
                "ORDER BY video_count DESC  LIMIT 10;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns = ['Channel Name', 'Video Count'])
                # df.columns = ['Channel Name', 'Video Count']
                st.write(df)
                fig = px.bar(df, x='Channel Name', y='Video Count', title='Top 10 Channels by Video Count')
                fig.update_layout(xaxis_title='Channel Name', yaxis_title='Video Count')

                # Display the chart using Streamlit
                st.plotly_chart(fig)
        elif selected_option == selected_query[3]:
            st.subheader(
                "What are the top 10 most viewed videos and their respective channels?"
            )
            query = (
                "SELECT channel.channel_name, video.view_count, video.video_name "
                "FROM video INNER JOIN playlist "
                "ON video.playlist_id = playlist.playlist_id "
                "JOIN channel ON playlist.channel_id = channel.channel_id "
                "ORDER BY view_count DESC limit 10;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns = ['Channel Name', 'View Count','Video Name'])
                st.write(df)
                fig = px.bar(df, x='Channel Name', y='View Count', color='Video Name',
                             title='Top 10 Videos by View Count',
                             labels={'View Count': 'Number of Views', 'Channel Name': 'Channel Name'})
                fig.update_layout(xaxis_title='Channel Name', yaxis_title='View Count')
                st.plotly_chart(fig)

        elif selected_option == selected_query[4]:
            st.subheader(
                "How many comments were made on each video, and what are their corresponding video names?"
            )
            query = "SELECT comment_count, video_name from video;"
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result, columns=['Comment count','Video Names'])
                st.write(df)
        elif selected_option == selected_query[5]:
            st.subheader(
                "Which videos have the highest number of likes, and what are their corresponding channel names?"
            )
            query = (
                "SELECT like_count,channel_name from video "
                "JOIN playlist ON video.playlist_id = playlist.playlist_id "
                "JOIN channel ON channel.channel_id = playlist.channel_id "
                "ORDER BY video.like_count DESC limit 10;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns=['Like count','Channel Names'])
                st.write(df)
                fig = px.bar(df, y='Like count', x='Channel Names', title='Top 10 Channels by like Count')
                fig.update_layout(xaxis_title='Channel Names', yaxis_title='Like count')

                # Display the chart using Streamlit
                st.plotly_chart(fig)
        elif selected_option == selected_query[6]:
            st.subheader(
                "What is the total number of likes and dislikes for each video, and what are their corresponding video names?"
            )
            query = "SELECT like_count,dislike_count,video_name FROM video;"
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns=['Like Count','Dislike Count','Video Name'])
                st.write(df)

        elif selected_option == selected_query[7]:
            st.subheader(
                "What is the total number of views for each channel, and what are their corresponding channel names?"
            )
            query = "SELECT channel_name,channel_views FROM channel;"
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns=['Channel Name','Views'])
                st.write(df)
                fig = px.bar(df, x='Channel Name', y='Views', title='Top  Channels by View Count')
                fig.update_layout(xaxis_title='Channel Name', yaxis_title='Views')
                st.plotly_chart(fig)
        elif selected_option == selected_query[8]:
            st.subheader(
                "What are the names of all the channels that have published videos in the year 2022?"
            )
            query = (
                "SELECT channel_name  ,video.video_name FROM channel "
                "JOIN playlist on channel.channel_id = playlist.channel_id "
                "join video on playlist.playlist_id = video.playlist_id "
                "WHERE YEAR(published_date)= 2022;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result, columns=['Channels Published Video in the year 2022','Video Name'])
                st.write(df)
        elif selected_option == selected_query[9]:
            st.subheader(
                "What is the average duration of all videos in each channel, and what are their corresponding channel names?"
            )
            query = (
                "SELECT AVG(duration)as average_duration ,channel_name FROM channel "
                "JOIN playlist on channel.channel_id = playlist.channel_id "
                "JOIN video ON playlist.playlist_id = video.playlist_id "
                "GROUP by channel_name;"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns=['Duration in seconds','Channel Names'])
                st.write(df)
        else:
            selected_option = selected_query[10]
            st.subheader(
                "Which videos have the highest number of comments, and what are their corresponding channel names?"
            )
            query = (
                "SELECT comment_count,channel_name FROM channel "
                "JOIN playlist on channel.channel_id = playlist.channel_id "
                "JOIN video ON playlist.playlist_id = video.playlist_id "
                "GROUP by channel_name "
                "ORDER BY video.comment_count DESC  LIMIT 5"
            )
            result = execute_mysql_query(query)
            if result:
                st.success("Query executed successfully.")
                df = pd.DataFrame(data=result,columns=['Highest Comment Count','Channel Name'])
                st.write(df)
                fig = px.bar(df, y='Highest Comment Count', x='Channel Name', title='Top Comment counts by Channels names')
                fig.update_layout(xaxis_title='Channel Name', yaxis_title='Highest Comment Count')
                st.plotly_chart(fig)


if __name__ == "__main__":
    create_database_if_not_exists()
    create_table()
    main()
