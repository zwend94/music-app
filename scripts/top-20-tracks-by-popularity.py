import time
import requests
import pandas as pd
from pyspark.sql import SparkSession

CLIENT_ID = "4e85871bfd71487db76f309451192a58"
CLIENT_SECRET = "5d2bd0dac36647ab8318a20e7710c5cb"

def get_spotify_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}
    response = requests.post(
        url, headers=headers, data=data, auth=(client_id, client_secret)
    )
    response.raise_for_status()
    return response.json().get("access_token")
# Print Token
print(get_spotify_token(client_id=CLIENT_ID,client_secret=CLIENT_SECRET))

def fetch_top_tracks(artist_id, token, market="US", limit=20):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"market": market}

    for attempt in range(5):  # Retry 5x
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:  # 200: Success, 403 invalid tokens, 404: error in call
            tracks = response.json().get("tracks", [])[:limit]
            return [
                {
                    "Artist_ID": artist_id,
                    "Track_ID": track.get("id"),
                    "Track_Name": track.get("name"),
                    "Popularity": track.get("popularity", 0),
                    "Duration_ms": track.get("duration_ms"),
                    "Explicit": track.get("explicit"),
                    "Album_Name": track.get("album", {}).get("name", "Unknown"),
                    "Album_Release_Date": track.get("album", {}).get("release_date", "Unknown"),
                    "Release_Date_Precision": track.get("album", {}).get("release_date_precision", "Unknown"),
                    "Track_Number": track.get("track_number", 0),
                    "Disc_Number": track.get("disc_number", 0),
                    "Available_Markets": len(track.get("available_markets", [])),
                    "External_URL": track.get("external_urls", {}).get("spotify", "Unknown"),
                    "Artists_Featured": ", ".join([artist["name"] for artist in track.get("artists", [])]),
                    "Album_Type": track.get("album", {}).get("album_type", "Unknown"),
                    "Is_Local": track.get("is_local", False),
                    "Href": track.get("href", "Unknown"),
                }
                for track in tracks
            ]

        elif response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get("Retry-After", 1))
            print(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)

        else:
            response.raise_for_status()

    print(f"Failed to fetch top tracks for artist {artist_id} after multiple retries.")
    return []
  
spark = SparkSession.builder.appName("SpotifyTrackFetcher").enableHiveSupport().getOrCreate()
# Validate session
print(spark.catalog)

# GET Spotify token
token = get_spotify_token(CLIENT_ID, CLIENT_SECRET)

artist_spark_df = spark.sql("SELECT * FROM artists_table2")

# Convert Spark DF to Pandas DF
artist_df = artist_spark_df.toPandas()

all_track_data = []
for _, row in artist_df.iterrows():
    artist_id = row["Artist_ID"]
    top_tracks = fetch_top_tracks(artist_id, token)
    time.sleep(0.5)  # Delay to for rate limits
    all_track_data.extend(top_tracks)

tracks_df = pd.DataFrame(all_track_data)

# Validate intermediate calc
print(tracks_df.head())

# Visualize 
import seaborn as sns
import matplotlib.pyplot as plt

# Filter data (Top 20 tracks by Popularity)
top_tracks = tracks_df.sort_values(by='Popularity', ascending=False).head(20)

# Create a barchart
plt.figure(figsize=(12, 8))
sns.barplot(
    x='Popularity', 
    y='Track_Name', 
    data=top_tracks, 
    palette='viridis'
)

# Set plot title & axis labels
plt.title('Top 20 Tracks by Popularity', fontsize=16)
plt.xlabel('Popularity', fontsize=12)
plt.ylabel('Track Name', fontsize=12)
plt.tight_layout()
plt.show()

# Convert to Spark DF and save
tracks_spark_df = spark.createDataFrame(tracks_df)
tracks_spark_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("default.top_tracks_table")

print("Top tracks data saved to Hive table 'default.top_tracks_table'.")
