'''
Permalink to Superset Dashboard:
http://localhost:8088/superset/dashboard/p/0aG1Z9r7Zyv/
'''
import pandas as pd
import json
import glob
from sqlalchemy import create_engine, text

# 1.load and combine .JSON files into a singular file
all_files = glob.glob('Streaming_History_Audio_*.json')
print(f"Found {len(all_files)} streaming history files")

all_data = []
for file in all_files:
    print(f"Loading {file}...")
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        all_data.extend(data)

# 2.convert to DataFrame and process
df = pd.DataFrame(all_data)

#this is a fix to a problem.I had earlier with timezones(changes to UTC)
df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize(None)

df['minutes_played'] = df['ms_played'] / 60000
df['date'] = df['ts'].dt.date
df['hour'] = df['ts'].dt.hour
df['day_of_week'] = df['ts'].dt.day_name()
df['year'] = df['ts'].dt.year

#only analyzes songs that were played for more than 30 seconds(not skipped)
df_full_plays = df[df['minutes_played'] >= 0.5].copy()

# 3.configure DB connectioj
db_user = 'root'
db_password = 'Tara2023!' # Change this to your actual password
db_host = 'localhost'
db_name = 'spotify_analytics'

# 4.upload to MySQL
try:
    #see if DB exists, if it does not then this will create it
    base_engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}")
    with base_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        conn.commit()

    #creates engine for data upload
    engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")

    print(f"Uploading {len(df_full_plays)} rows to MySQL...")
    #this ideally assists with larger data uploads
    df_full_plays.to_sql('streaming_history', con=engine, if_exists='replace', index=False, chunksize=5000)

    print("Process Complete. Data is now in MySQL.")

except Exception as e:
    print(f"An error occurred: {e}")