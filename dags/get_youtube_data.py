import json
import requests
import csv
import io
from minio import Minio
from minio.error import S3Error
import re
import pandas as pd
from io import BytesIO
from datetime import datetime









def clean_string(text):
    # Define a regex pattern to match whitespace, emojis, and special characters at the start and end
    pattern = r'^[\s\W]+|[\s\W]+$'
    # Substitute matches with an empty string
    cleaned_text = re.sub(pattern, '', text)
    return cleaned_text


def get(code: str, artist: str, track: str):
    track_ = clean_string(track.lower())

    url = None
    if artist is None:
        url = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet%2C%20id&q={track_}&type=video&videoCategoryId=10&key=<your_youtube_key>&safeSearch=strict&order=relevance&maxResults=50"
    else:
        artist_ = clean_string(artist.lower())
        url = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet%2C%20id&q={track_} {artist_}&type=video&videoCategoryId=10&key=<your_youtube_key>&safeSearch=strict&order=relevance&maxResults=50"

    print(url)

    try:

        payload = {}
        headers = {
          'Accept': 'application/json'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        dictResult = json.loads(response.text)
        # with open(f"result_youtube_sample_{code}.json", 'w') as fi:
        #     json.dump(dictResult, fi, indent=2)

        # MinIO setup (your MinIO instance configuration)
        minio_client = Minio(
            "localhost:9000",  # MinIO server URL (change to your instance)
            access_key="AAAAAAAAAAAAAAAAAAAA",  # Replace with your MinIO access key
            secret_key="BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",  # Replace with your MinIO secret key
            secure=False  # Set to True if using https
        )
        json_data = json.dumps(dictResult).encode("utf-8")  # Encode as bytes
        # Specify bucket name and object name
        bucket_name = "starrocks"  # Replace with your bucket name
        date_now = datetime.now()
        year_ = date_now.year
        month_ = date_now.month
        day_ = date_now.strftime("%d")
        object_name = f"datalake/youtube/{year_}/{month_}/{day_}/raw_internal_code_{code}.json"  # Replace with desired object name

        # Upload JSON data directly as an object
        try:
            # Make sure the bucket exists
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)

            # Write JSON data to MinIO
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=BytesIO(json_data),
                length=len(json_data),
                content_type="application/json"
            )
            print(f"Successfully uploaded {object_name} to {bucket_name}")

        except Exception as e:
            print(f"Error uploading JSON data to MinIO: {e}")

        if not dictResult["items"]:
            pass
        else:
            num = 0
            for x in dictResult["items"]:
                information_required = {
                    "request_url": url,
                    "code": code,
                    "video_id": x["id"]["videoId"],
                    "published_at": x["snippet"]["publishedAt"],
                    "channel_id": x["snippet"]["channelId"],
                    "channel_title": x["snippet"]["channelTitle"],
                    "name": x["snippet"]["title"]
                }

                # Specify the CSV file path
                num = num + 1
                csv_file_path = f"result/youtube/output_youtube_{code}_{num}.csv"

                # Get the headers from the first dictionary's keys
                headers = information_required.keys()

                # Write to CSV with double quotes around values
                with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=headers, delimiter='|')
                    writer.writeheader()  # Write header
                    writer.writerows([information_required])  # Write rows

    except Exception as e:
        print(e)

def main_youtube():
    df = pd.read_csv("sampleData.csv")
    df = df.where(pd.notna(df), None)

    df_dict = df.to_dict(orient='records')
    # print(df_dict)

    for x in df_dict:
        print(x)
        print("===========RUNNING YOUTUBE GET DATA===========")
        get(x["CODE"], x["ORIGINAL_ARTIST"], x["SONG_TITLE"])