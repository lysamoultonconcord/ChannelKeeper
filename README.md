# Channel Master Streamlit App

## Setup
1) Create a venv
2) Install deps
   pip install -r requirements.txt
3) Create .streamlit/secrets.toml (see template)
4) Run:
   streamlit run app.py

## What it does
- Lookup by CHANNEL_ID in Snowflake
- Pulls YouTube channel title + publishedAt (DATE_CREATED) via YouTube Data API
- Computes URL as https://www.youtube.com/channel/<CHANNEL_ID>
- Lets you edit the remaining fields
- Saves using MERGE upsert into BUS_CORPORATE.RIGHTS_OPTIMIZATION.CHANNEL_MASTER
