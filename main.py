import streamlit as st
import requests
import pandas as pd
import time
import datetime

def get_unixtime_from_datetime(dt):
    """Converts a datetime object to UNIX time."""
    return int(time.mktime(dt.timetuple()))

def get_comments(post_id, owner_id, access_token):
    """Gets comments for a post."""
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
        st.error(f"Error getting comments: {e}")
        return []

def get_vk_newsfeed(query, start_time, end_time, access_token, include_comments, progress_bar):
    df = pd.DataFrame()
    count = "200"

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d").date()
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d").date()

    delta = datetime.timedelta(days=1)
    current_date = start_time

    n_days = int((end_time - start_time).days) + 1

    for _ in range(n_days):
        url = (
            f"https://api.vk.com/method/newsfeed.search?q={query}"
            f"&count={count}"
            f"&access_token={access_token}"
            f"&start_time={get_unixtime_from_datetime(current_date)}"
            f"&end_time={get_unixtime_from_datetime(current_date + delta)}"
            f"&v=5.131"
        )

        try:
            res = requests.get(url)
            json_text = res.json()

            if 'response' in json_text and 'items' in json_text['response']:
                for item in json_text['response']['items']:
                    post_id = item['id']
                    owner_id = item['owner_id']

                    if include_comments:
                        comments = get_comments(post_id, owner_id, access_token)
                        item['comments'] = comments

                items_df = pd.json_normalize(json_text['response']['items'], sep='_')
                df = pd.concat([df, items_df], ignore_index=True)

            else:
                st.warning(f"No data in response for date {current_date}")

        except Exception as e:
            st.error(f"Error executing request: {e}")

        progress_bar.progress((current_date - start_time).days / n_days)
        time.sleep(0.5)  # Delay between requests
        current_date += delta

    df.fillna('', inplace=True)
    return df

def main():
    st.title("VK News and Comments Scraper")

    # Input fields
    access_token = st.text_input("Enter your VK API access token", type="password")
    query = st.text_input("Enter keyword or expression")
    start_date = st.date_input("Start date")
    end_date = st.date_input("End date")
    include_comments = st.checkbox("Include comments")

    if st.button("Start Fetching"):
        if not access_token:
            st.error("Please enter a VK API access token.")
        elif not query:
            st.error("Please enter a keyword or expression.")
        elif end_date < start_date:
            st.error("End date must be after start date.")
        else:
            progress_bar = st.progress(0)
            data = get_vk_newsfeed(query, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), 
                                   access_token, include_comments, progress_bar)
            
            if not data.empty:
                st.success(f"Fetched {len(data)} posts.")
                st.dataframe(data)
                
                csv = data.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name="vk_data.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No data was fetched. Try adjusting your search parameters.")

if __name__ == "__main__":
    main()