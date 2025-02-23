import requests
import pandas as pd
from pyspark.sql import SparkSession

CLIENT_ID = "01610abcafbc4bc6899f1217cd4407a9"
CLIENT_SECRET = "0ad83c8b2e3642b38a7ad2aaa58ab3ed"

# Genres to process
GENRES = [
    "pop-punk",
    "metalcore"
    
]

# Get token
def get_spotify_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}
    response = requests.post(
        url, headers=headers, data=data, auth=(client_id, client_secret)
    )
    response.raise_for_status()
    return response.json().get("access_token")

# Get top artists by genre
def fetch_top_artists_by_genre(genre, token, limit=50):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"q": f"genre:{genre}", "type": "artist", "limit": limit}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("artists", {}).get("items", [])

# Get ALL artists by genre
def fetch_all_artists_by_genres(genres, token, limit=50):
    """
    Fetch artists for multiple genres and append to a global list.

    Args:
    - genres (list): List of genres to process.
    - token (str): Spotify API token.
    - limit (int): Number of artists to fetch per genre (default: 50).

    Returns:
    - List of artist data dictionaries.
    """
    # Data storage
    artist_data = []

    for genre in genres:
        print(f"Fetching artists for genre: {genre}")
        artists = fetch_top_artists_by_genre(genre, token, limit)

        if not artists:
            print(f"No artists found for genre: {genre}")
            continue

        for artist in artists:
            artist_data.append(
                {
                    "Artist_Name": artist.get("name", "Unknown"),  # Cleaned column names
                    "Artist_ID": artist.get("id", "Unknown"),
                    "Genre": genre,
                    "Popularity": artist.get("popularity", 0),
                    "Followers": artist.get("followers", {}).get("total", 0),
                    "Spotify_URL": artist.get("external_urls", {}).get("spotify", ""),
                }
            )
    return artist_data

#Initialize Spark
spark = SparkSession.builder \
    .appName("Spotify Artist Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Get spotify token
token = get_spotify_token(CLIENT_ID, CLIENT_SECRET)

# Get artist data
artist_data = fetch_all_artists_by_genres(GENRES, token, limit=50)

# Validate data pulll
if not artist_data:
    print("No artist data fetched. Please check the API or genre list.")
else:
    # Convert to Pandas DF
    artist_df = pd.DataFrame(artist_data)
    print("\nArtists Data (Pandas DataFrame):")
    print(artist_df.head())
    artist_df = artist_df.drop_duplicates(subset=["Artist_Name"], keep="first")

    # Pandas DF to Spark DF
    spark_df = spark.createDataFrame(artist_df)

    # Clean columns in Spark DF
    for col in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(col, col.replace(" ", "_").replace(".", "_"))

    print("\nSpark DataFrame Schema:")
    spark_df.printSchema()

    # Save to Hive table w/ schema merge
    spark_df.write.option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable("default.artists_table2")

    print("Data saved to Hive metastore with schema merge.")

# Remove duplicate artist names keeping the first occurrence
if not artist_df.empty:
    artist_df = artist_df.drop_duplicates(subset=["Artist_Name"], keep="first")
    print("\nArtists Data After Removing Duplicates (Pandas DataFrame):")
    print(artist_df.head())
else:
    print("No data available to process.")