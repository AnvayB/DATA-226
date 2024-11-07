import pandas as pd
import json

def collapse_genres(j):
    genres = []
    ar = json.loads(j)
    for a in ar:
        genres.append(a.get("name"))
    return " ".join(sorted(genres))

def combine_features(row):
    try:
        return row['overview'] + " " + row["genres_name"]
    except:
        print("Error:", row)

def process_tmdb_csv(input_file, output_file):
    # Load the TMDB data
    movies = pd.read_csv(input_file)

    # Process the genres into a single string
    movies['genres_name'] = movies['genres'].apply(collapse_genres)
    for f in ['original_title', 'overview', 'genres_name']:
        movies[f] = movies[f].fillna('')

    # Combine overview and genres for the text field
    movies["text"] = movies.apply(combine_features, axis=1)

    # Select and rename necessary columns
    movies = movies[['id', 'original_title', 'text']]
    movies.rename(columns={'original_title': 'title', 'id': 'doc_id'}, inplace=True)

    # Format data for Vespa
    vespa_data = []
    for _, row in movies.iterrows():
        vespa_record = {
            "put": f"id:hybrid-search:doc::{row['doc_id']}",
            "fields": {
                "doc_id": row['doc_id'],
                "title": row['title'],
                "text": row['text']
            }
        }
        vespa_data.append(vespa_record)

    # Write to JSONL file
    with open(output_file, 'w') as file:
        for record in vespa_data:
            file.write(json.dumps(record) + '\n')

# Example usage
process_tmdb_csv("tmdb_5000_movies.csv", "clean_tmdb_vespa.jsonl")
