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

def parse_duration(duration_string):
    if duration_string:
        seconds_match = re.search(r"(\d+)S", duration_string)
        minutes_match = re.search(r"(\d+)M", duration_string)
        minutes = int(minutes_match.group(1)) if minutes_match else 0
        seconds = int(seconds_match.group(1)) if seconds_match else 0
        total_seconds = minutes * 60 + seconds
        return total_seconds

def get_channel_playlists_videos_and_comments(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)

    try:
        channel_info = (
            youtube.channels()
            .list(
                part="snippet,statistics",
                id=channel_id,
            )
            .execute()
        )

        if "items" in channel_info and channel_info["items"]:
            channel_data = {
                "channel_id": channel_id,
                "channel_name": channel_info["items"][0]["snippet"]["title"],
                "channel_type": "",
                "channel_views": channel_info["items"][0]["statistics"]["viewCount"],
                "channel_description": channel_info["items"][0]["snippet"][
                    "description"
                ],
                "channel_status": "",
            }

            branding_settings = channel_info["items"][0].get("brandingSettings", {})
            if "channel" in branding_settings:
                channel_data["channel_type"] = branding_settings["channel"].get(
                    "type", ""
                )
                channel_data["channel_status"] = branding_settings["channel"].get(
                    "status", ""
                )
        else:
            print("No channel information found.")
            return None

    except HttpError as e:
        print(f"Error fetching channel information: {e}")
        return None

    # Fetch playlists from the channel
    try:
        playlists = (
            youtube.playlists()
            .list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=10,  # Adjust the number of playlists you want to fetch
            )
            .execute()
        )

        playlist_list = []

        for playlist in playlists.get(
            "items", []
        ):  # Use get() to handle potential missing "items"
            playlist_data = {
                "playlist_id": playlist["id"],
                "channel_id": channel_id,
                "playlist_name": playlist["snippet"]["title"],
            }

            # Fetch videos from the playlist
            try:
                playlist_videos = (
                    youtube.playlistItems()
                    .list(
                        part="snippet",
                        playlistId=playlist["id"],
                        maxResults=10,  # Adjust the number of videos you want to fetch per playlist
                    )
                    .execute()
                )

                video_list = []

                for playlist_item in playlist_videos.get(
                    "items", []
                ):  # Use get() to handle potential missing "items"
                    video_id = playlist_item["snippet"]["resourceId"]["videoId"]

                    # Fetch video details
                    try:
                        video_details = (
                            youtube.videos()
                            .list(
                                part="snippet,statistics,contentDetails",
                                id=video_id,
                            )
                            .execute()
                        )

                        snippet = video_details.get("items", [])
                        if snippet:
                            snippet = snippet[0].get("snippet", {})
                        else:
                            snippet = {}

                        content_details = video_details.get("items", [])
                        if content_details:
                            content_details = content_details[0].get("contentDetails", {})
                        else:
                            content_details = {}

                        statistics = video_details.get("items", [])
                        if statistics:
                            statistics = statistics[0].get("statistics", {})
                        else:
                            statistics = {}



                        # Check if "title" exists before accessing it
                        video_name = snippet.get("title", None)
                        video_description = snippet.get("description", None)
                        duration=content_details.get("duration",0)
                        view_count= statistics.get("viewCount",0)
                        like_count= statistics.get("likeCount",0)
                        dislike_count=statistics.get("dislikeCount",0)
                        favorite_count= statistics.get("favoriteCount",0)
                        comment_count=statistics.get("commentCount",0)
                        published_at = snippet.get("publishedAt", None)
                        if published_at:
                           timestamp_dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
                           published_at = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
                        video_comments = []
                        try:
                            video_comments = (
                                youtube.commentThreads()
                                .list(
                                    part="snippet",
                                    videoId=video_id,
                                    order="time",
                                    textFormat="plainText",
                                    maxResults=25,
                                )
                                .execute()
                            )
                        except HttpError as comment_error:
                            if comment_error.resp.status == 404:
                                print(f"Comments not found for video: {video_id}")
                            else:
                                print(f"Error fetching comments: {comment_error}")

                        comment_list = []
                        if video_comments:
                            for comment in video_comments.get(
                                "items", []
                            ):  # Use get() to handle potential missing "items"
                                comment_data = {
                                    "comment_id": comment["id"],
                                    "video_id": video_id,
                                    "comment_text": comment["snippet"][
                                        "topLevelComment"
                                    ]["snippet"]["textDisplay"],
                                    "comment_author": comment["snippet"][
                                        "topLevelComment"
                                    ]["snippet"]["authorDisplayName"],
                                    "comment_published_date": comment["snippet"][
                                        "topLevelComment"
                                    ]["snippet"]["publishedAt"],
                                }
                                comment_list.append(comment_data)

                        video_data = {
                            "video_id": video_id,
                            "playlist_id": playlist_data["playlist_id"],
                            "video_name": video_name,
                            "comments": comment_list,
                            "video_description": video_description,
                            "duration":parse_duration(duration),
                            "view_count":view_count,
                            "like_count":like_count,
                            "dislike_count":dislike_count,
                            "favorite_count":favorite_count,
                            "comment_count":comment_count,
                            "published_at":published_at
                            # Add more video details as needed
                        }

                        video_list.append(video_data)

                    except HttpError as video_error:
                        if video_error.resp.status == 404:
                            print(f"Video not found: {video_id}")
                        else:
                            print(f"Error fetching video details: {video_error}")

                playlist_data["videos"] = video_list
                playlist_list.append(playlist_data)

            except HttpError as playlist_error:
                print(f"Error fetching playlist videos: {playlist_error}")

    except HttpError as playlists_error:
        print(f"Error fetching playlists: {playlists_error}")

    return {
        "channel_data": channel_data,
        "playlists": playlist_list,
    }
