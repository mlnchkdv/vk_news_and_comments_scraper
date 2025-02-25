import streamlit as st
import requests
import pandas as pd
import time
import datetime

def get_unixtime_from_datetime(dt):
    return int(time.mktime(dt.timetuple()))

def get_comments(post_id, owner_id, access_token):
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

def get_vk_newsfeed(query, start_time, end_time, access_token, include_comments, progress_bar, time_sleep):
    df = pd.DataFrame()
    count = "200"

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d").date()
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d").date()

    delta = datetime.timedelta(days=1)
    current_time = start_time

    n_days = int((end_time - start_time).days) + 1
    total_requests = n_days * 24  # Assuming one request per hour

    request_count = 0

    while current_time <= end_time:
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

        request_count += 1
        progress = min(request_count / total_requests, 1.0)
        progress_bar.progress(progress)

        time.sleep(time_sleep)
        current_time += delta

    return df

def main():
    st.title("VK News and Comments Parser")

    access_token = st.text_input("Enter your VK API access token:", type="password")
    query = st.text_input("Enter keyword or expression:")
    start_date = st.date_input("Start date:")
    end_date = st.date_input("End date:")
    include_comments = st.checkbox("Include comments", value=True)
    time_sleep = st.slider("Time sleep between requests (seconds)", min_value=0.1, max_value=5.0, value=0.5, step=0.1)

    columns = [
        'attachments', 'date', 'from_id', 'id', 'owner_id', 'text', 'track_code', 'comments_count', 
        'likes_count', 'post_source_platform', 'post_source_type', 'reposts_count', 'views_count', 
        'donut_miniapp_url', 'signer_id', 'geo_coordinates', 'geo_place_discriminator', 'geo_place_created', 
        'geo_place_id', 'geo_place_is_deleted', 'geo_place_latitude', 'geo_place_longitude', 'geo_place_title', 
        'geo_place_total_checkins', 'geo_place_updated', 'geo_place_country', 'geo_place_category', 
        'geo_place_category_object_id', 'geo_place_category_object_title', 'geo_place_category_object_icons', 
        'geo_type', 'copyright_link', 'copyright_name', 'copyright_type', 'copyright_id', 'edited', 
        'geo_place_address', 'author_ad_advertiser_info_url', 'author_ad_ad_marker'
    ]

    selected_columns = st.multiselect("Select columns to save in CSV", columns, default=columns)

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
                             access_token, include_comments, progress_bar, time_sleep)
        status_text.text("Parsing completed!")

        if not df.empty:
            # Convert Unix timestamp to readable date
            df['date'] = pd.to_datetime(df['date'], unit='s')

            # Filter columns
            df = df[[col for col in selected_columns if col in df.columns]]

            st.write(df)

            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="vk_newsfeed.csv",
                mime="text/csv",
            )

            if include_comments:
                st.subheader("Comments")
                for _, row in df.iterrows():
                    st.write(f"Post ID: {row['id']}")
                    if 'comments' in row and row['comments']:
                        comments_df = pd.DataFrame(row['comments'])
                        st.write(comments_df)
                    else:
                        st.write("No comments for this post.")
                    st.write("---")
        else:
            st.warning("No data found for the given parameters.")

if __name__ == "__main__":
    main()