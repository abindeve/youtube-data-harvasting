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

def process_and_save_channel_data():
    mongo_client = connect_mongodb()
    mongodb_db = mongo_client["youtube_data"]
    mysql_connection = connect_mysql()

    latest_channel_data = mongodb_db["channels"].find_one(sort=[("_id", -1)])

    if latest_channel_data:
        save_status = save_channel_data_to_mysql(mysql_connection, latest_channel_data)
        if save_status == 1:
            for playlist_data in mongodb_db["playlists"].find(
                {"channel_id": latest_channel_data["channel_id"]}
            ):
                save_playlist_to_mysql(mysql_connection, playlist_data)

                for video_data in mongodb_db["videos"].find(
                    {"playlist_id": playlist_data["playlist_id"]}
                ):
                    save_video_to_mysql(mysql_connection, video_data)

                    video_id = video_data["video_id"]
                    comments = list(mongodb_db["comments"].find({"video_id": video_id}))

                    if comments:
                        for comment_data in comments:
                            save_comment_to_mysql(mysql_connection, comment_data)

    mongo_client.close()
    mysql_connection.close()
def save_playlist_to_mysql(mysql_connection, playlist_data):
    cursor = mysql_connection.cursor()

    cursor.execute(
        """
        INSERT INTO playlist (playlist_id, channel_id, playlist_name)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE playlist_name=%s
    """,
        (
            playlist_data["playlist_id"],
            playlist_data["channel_id"],
            playlist_data["playlist_name"],
            playlist_data["playlist_name"],
        ),
    )
    mysql_connection.commit()
    cursor.close()


# Function to save video data to MySQL
#  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
# , video_description,
#                        published_date, view_count, like_count, dislike_count,
#                        favorite_count, comment_count, duration, thumbnail,
#                        caption_status
def save_video_to_mysql(mysql_connection, video_data):
    cursor = mysql_connection.cursor()
    cursor.execute(
        """
        INSERT INTO video (video_id, playlist_id, video_name,video_description,duration,view_count,like_count,dislike_count,comment_count,favorite_count,published_date)
        VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)
                   
       
    """,
        (
            video_data["video_id"],
            video_data["playlist_id"],
            video_data["video_name"],
            video_data["video_description"],
            video_data['duration'],
            video_data['view_count'],
            video_data['like_count'],
            video_data['dislike_count'],
            video_data['comment_count'],
            video_data['favorite_count'],
            video_data['published_at'],
            # video_data['favorite_count'],
            # video_data['comment_count'],
           
            # video_data['thumbnail'],
            # video_data['caption_status'],
        ),
    )
    mysql_connection.commit()
    cursor.close()


# Function to save comment data to MySQL
def save_comment_to_mysql(mysql_connection, comment_data):
    cursor = mysql_connection.cursor()
    cursor.execute(
        """
        INSERT INTO comment (comment_id, video_id, comment_text, comment_author,
                             comment_published_date)
        VALUES (%s, %s, %s, %s, %s)
       
    """,
        (
            comment_data["comment_id"],
            comment_data["video_id"],
            comment_data["comment_text"],
            comment_data["comment_author"],
            comment_data["comment_published_date"],
        ),
    )
    mysql_connection.commit()
    cursor.close()


def save_channel_data_to_mysql(mysql_connection, channel_data):
    cursor = mysql_connection.cursor()
    cursor.execute(
        """
    SELECT channel_id FROM channel WHERE channel_id = %s
""",
        (channel_data["channel_id"],),
    )

    existing_channel = cursor.fetchone()
    if existing_channel:
        return 0
    else:
        cursor.execute(
            """
            INSERT INTO channel (
                channel_id, channel_name, channel_type, channel_views, 
                channel_description, channel_status
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
            (
                channel_data["channel_id"],
                channel_data["channel_name"],
                channel_data["channel_type"],
                channel_data["channel_views"],
                channel_data["channel_description"],
                channel_data["channel_status"],
            ),
        )

    mysql_connection.commit()
    cursor.close()
    return 1

