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
        if 'response' in json_text and 'items' in json_text['response']:
            return json_text['response']['items']
        else:
            return []
    except Exception as e:
        st.error(f"Error fetching comments: {e}")
        return []

def get_vk_newsfeed(query, start_time, end_time, access_token, include_comments, progress_bar, time_sleep):
    df = pd.DataFrame()
    all_comments = []
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
                        if comments:
                            for comment in comments:
                                comment['post_id'] = post_id
                                comment['post_owner_id'] = owner_id
                            all_comments.extend(comments)

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

    comments_df = pd.DataFrame(all_comments) if all_comments else pd.DataFrame()
    return df, comments_df

def main():
    st.title("VK News and Comments Parser")

    access_token = st.text_input("Enter your VK API access token:", type="password")
    query = st.text_input("Enter keyword or expression:")
    start_date = st.date_input("Start date:")
    end_date = st.date_input("End date:")
    include_comments = st.checkbox("Include comments", value=True)
    time_sleep = st.slider("Time sleep between requests (seconds)", min_value=0.1, max_value=5.0, value=0.5, step=0.1)

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
        df, comments_df = get_vk_newsfeed(query, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), 
                                          access_token, include_comments, progress_bar, time_sleep)
        status_text.text("Parsing completed!")

        if not df.empty:
            # Convert Unix timestamp to readable date
            df['date'] = pd.to_datetime(df['date'], unit='s')

            # Allow user to select columns after data is loaded
            all_columns = df.columns.tolist()
            selected_columns = st.multiselect("Select columns to display and save", all_columns, default=all_columns)

            # Filter columns
            df_display = df[selected_columns]

            st.subheader("Posts")
            st.write(df_display)

            csv = df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Posts CSV",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )

            if include_comments and not comments_df.empty:
                st.subheader("Comments")
                display_comments = st.checkbox("Display comments table", value=False)
                
                if display_comments:
                    comments_df['date'] = pd.to_datetime(comments_df['date'], unit='s')
                    st.write(comments_df)

                    comments_csv = comments_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Comments CSV",
                        data=comments_csv,
                        file_name="vk_comments.csv",
                        mime="text/csv",
                    )
        else:
            st.warning("No data found for the given parameters.")

if __name__ == "__main__":
    main()