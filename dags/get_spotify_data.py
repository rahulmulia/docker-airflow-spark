import json
import requests
from get_token import generate_token
import csv
import io
from minio import Minio
from minio.error import S3Error
import re
import pandas as pd
from io import BytesIO
from datetime import datetime


def get_token():
    tokenFile = open("token.json", 'r')
    tokenData = json.loads(tokenFile.read())
    return tokenData["access_token"]


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
        url = f"https://api.spotify.com/v1/search?q=track:{track_}&type=track"
    else:
        artist_ = clean_string(artist.lower())
        url = f"https://api.spotify.com/v1/search?q=artist:{artist_}%20track:{track_}&type=track"

    print(url)

    try :

        payload = {}
        headers = {
          'Authorization': f'Bearer {get_token()}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        dictResult = json.loads(response.text)
        # with open(f"result_spotify_sample_{code}.json", 'w') as fi:
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
        object_name = f"datalake/spotify/{year_}/{month_}/{day_}/raw_internal_code_{code}.json"  # Replace with desired object name

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

        if not dictResult["tracks"]["items"]:
            pass
        else:
            num = 0
            for x in dictResult["tracks"]["items"]:
                information_required = {
                    "request_url": url,
                    "code": code,
                    "album_id": x["album"]["id"],
                    "album_type": x["album"]["album_type"],
                    "artists_name":  ",".join([ x["name"] for x in x["artists"] ]) if len(x["artists"]) > 1 else x["artists"][0]["name"],
                    "total_tracks": x["album"]["total_tracks"],
                    "available_markets": ",".join(x["available_markets"]),
                    "release_date": x["album"]["release_date"],
                    "disc_number": x["disc_number"],
                    "duration_ms": x["duration_ms"],
                    "isrc": x["external_ids"]["isrc"],
                    "spotify_id": x["id"],
                    "name": x["name"],
                    "type": x["type"],
                    "spotify_uri": x["uri"]
                }

                # Specify the CSV file path
                num = num + 1
                csv_file_path = f"result/spotify/output_spotify_{code}_{num}.csv"

                # Get the headers from the first dictionary's keys
                headers = information_required.keys()

                # Write to CSV with double quotes around values
                with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=headers, delimiter='|')
                    writer.writeheader()  # Write header
                    writer.writerows([information_required])  # Write rows

    except Exception as e:
        print(e)
        generate_token()

def main_spotify():
    df = pd.read_csv("sampleData.csv")
    df = df.where(pd.notna(df), None)

    df_dict = df.to_dict(orient='records')
    # print(df_dict)

    for x in df_dict:
        print(x)
        print("===========RUNNING SPOTIFY GET DATA===========")
        get(x["CODE"], x["ORIGINAL_ARTIST"], x["SONG_TITLE"])



