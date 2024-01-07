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




def create_table():
    create_table_channel = """
    CREATE TABLE IF NOT EXISTS channel (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, 
        channel_type VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        channel_views INT,
        channel_description TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, 
        channel_status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    )ENGINE=InnoDB;"""

    create_table_playlist = """
    CREATE TABLE IF NOT EXISTS playlist (
        playlist_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    )ENGINE=InnoDB;"""

    create_table_comment = """
    CREATE TABLE IF NOT EXISTS comment (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        comment_author VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        comment_published_date DATETIME
    )ENGINE=InnoDB;"""
    create_table_video = """
        CREATE TABLE IF NOT EXISTS video (
            video_id VARCHAR(255) PRIMARY KEY,
            playlist_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            published_date DATETIME,
            view_count INT,
            like_count INT,
            dislike_count INT,
            favorite_count INT,
            comment_count INT,
            duration INT,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            )ENGINE=InnoDB;"""
    try:
        execute_mysql_query(create_table_channel)
        execute_mysql_query(create_table_playlist)
        execute_mysql_query(create_table_comment)
        execute_mysql_query(create_table_video)
    except mysql.connector.Error as err:
        st.write(f"Error: {err}")


def connect_sql_databse():
    connection = mysql.connector.connect(
        host="localhost", user="root", password="", database="abin"
    )
    return connection, connection.cursor()


def execute_mysql_query(query):
    connection, cursor = connect_sql_databse()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result


# Function to save data to MySQL
def save_to_mysql(channel_data):
    connection, cursor = connect_sql_databse()
    query = """INSERT INTO channel_data (title, description, subscriber_count, video_count)
               VALUES (%s, %s, %s, %s)"""
    values = (
        channel_data["title"],
        channel_data["description"],
        channel_data["subscriber_count"],
        channel_data["video_count"],
    )
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    connection.close()

