import streamlit as st
import requests
import pandas as pd
from tqdm import tqdm
import time
import datetime
from io import BytesIO

def get_unixtime_from_datetime(dt):
    """Converts a datetime object to UNIX timestamp."""
    return int(time.mktime(dt.timetuple()))

def get_comments(post_id, owner_id, access_token):
    """Fetches comments for a post."""
    url = (
        f"https://api.vk.com/method/wall.getComments?"
        f"owner_id={owner_id}&post_id={post_id}&access_token={access_token}&v=5.131"
    )
    try:
        res = requests.get(url)
        json_text = res.json()
        if 'response' in json_text:
            return json_text['response']['items']
        else:
            return []
    except Exception as e:
        st.error(f"Error fetching comments: {e}")
        return []

def get_vk_newsfeed(query, start_time, end_time, access_token, include_comments, progress_bar):
    df = pd.DataFrame()
    count = "200"

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d").date()
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d").date()

    delta = datetime.timedelta(days=1)
    current_time = start_time

    n_days = int((end_time - start_time).days) + 1

    for _ in range(n_days):
        url = (
            f"https://api.vk.com/method/newsfeed.search?q={query}"
            f"&count={count}"
            f"&access_token={access_token}"
            f"&start_time={get_unixtime_from_datetime(current_time)}"
            f"&end_time={get_unixtime_from_datetime(current_time + delta)}"
            f"&v=5.131"
        )

        try:
            res = requests.get(url)
            json_text = res.json()

            if 'response' in json_text and 'items' in json_text['response']:
                for item in json_text['response']['items']:
                    if include_comments:
                        post_id = item['id']
                        owner_id = item['owner_id']
                        comments = get_comments(post_id, owner_id, access_token)
                        item['comments'] = comments

                items_df = pd.json_normalize(json_text['response']['items'], sep='_')
                df = pd.concat([df, items_df], ignore_index=True)

            else:
                st.warning(f"No data in response for {current_time}")

        except Exception as e:
            st.error(f"Error executing request: {e}")

        progress_bar.progress((current_time - start_time).days / n_days)
        time.sleep(0.5)  # Reduced delay to 0.5 seconds
        current_time += delta

    df.fillna('', inplace=True)
    return df

def main():
    st.title("VK News and Comments Parser")

    # User inputs
    access_token = st.text_input("Enter your VK API access token:", type="password")
    query = st.text_input("Enter keyword or expression:")
    start_date = st.date_input("Start date:")
    end_date = st.date_input("End date:")
    include_comments = st.checkbox("Include comments", value=True)

    if st.button("Start Parsing"):
        if not access_token or not query or not start_date or not end_date:
            st.error("Please fill in all fields.")
            return

        if (end_date - start_date).days < 1:
            st.error("The minimum parsing period should be at least 1 day.")
            return

        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Parsing in progress...")
        df = get_vk_newsfeed(query, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), 
                             access_token, include_comments, progress_bar)
        status_text.text("Parsing completed!")

        if not df.empty:
            st.write(df)

            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="vk_newsfeed.csv",
                mime="text/csv",
            )
        else:
            st.warning("No data found for the given parameters.")

if __name__ == "__main__":
    main()