import streamlit as st
import requests
import pandas as pd
import time
import datetime
import re

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

def get_vk_newsfeed(queries, start_datetime, end_datetime, access_token, include_comments, progress_bar, time_sleep):
    df = pd.DataFrame()
    all_comments = []
    count = "200"

    delta = datetime.timedelta(days=1)
    current_time = start_datetime

    total_seconds = (end_datetime - start_datetime).total_seconds()
    start_time = time.time()

    while current_time <= end_datetime:
        for query in queries:
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
                        item['matched_query'] = query
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
                    st.warning(f"No data in response for {query} at {current_time}")

            except Exception as e:
                st.error(f"Error executing request: {e}")

            time.sleep(time_sleep)

        elapsed_time = time.time() - start_time
        progress = min(elapsed_time / total_seconds, 1.0)
        progress_bar.progress(progress)

        current_time += delta

    comments_df = pd.DataFrame(all_comments) if all_comments else pd.DataFrame()
    return df, comments_df

def display_post_with_comments(post, comments):
    st.write(f"**Post ID:** {post['id']}")
    st.write(f"**Date:** {post['date']}")
    st.write(f"**Text:** {post['text']}")
    st.write(f"**Matched Query:** {post['matched_query']}")
    st.write(f"**Likes:** {post.get('likes_count', 'N/A')}")
    st.write(f"**Views:** {post.get('views_count', 'N/A')}")
    st.write(f"**Reposts:** {post.get('reposts_count', 'N/A')}")
    st.write("**Comments:**")
    for comment in comments:
        st.text(f"{comment['from_id']} ({comment['date']}): {comment['text']}")
    st.write("---")

def main():
    st.title("VK News and Comments Parser")

    access_token = st.text_input("Enter your VK API access token:", type="password")
    queries = st.text_area("Enter keywords or expressions (one per line):")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start date:")
    with col2:
        start_time = st.time_input("Start time:")
    with col3:
        end_date = st.date_input("End date:")
    with col4:
        end_time = st.time_input("End time:")
    
    start_datetime = datetime.datetime.combine(start_date, start_time)
    end_datetime = datetime.datetime.combine(end_date, end_time)
    
    include_comments = st.checkbox("Include comments", value=True)
    time_sleep = st.slider("Time sleep between requests (seconds)", min_value=0.1, max_value=6.0, value=0.5, step=0.1)

    if 'full_df' not in st.session_state:
        st.session_state.full_df = None
    if 'comments_df' not in st.session_state:
        st.session_state.comments_df = None

    if st.button("Start Parsing"):
        if not access_token or not queries or not start_date or not end_date:
            st.error("Please fill in all fields.")
            return

        if (end_datetime - start_datetime).total_seconds() < 86400:  # 86400 seconds in a day
            st.error("The minimum parsing period should be at least 1 day.")
            return

        queries_list = [q.strip() for q in queries.split('\n') if q.strip()]

        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Parsing in progress...")
        df, comments_df = get_vk_newsfeed(queries_list, start_datetime, end_datetime, 
                                          access_token, include_comments, progress_bar, time_sleep)
        status_text.text("Parsing completed!")

        if not df.empty:
            # Convert Unix timestamp to readable date
            df['date'] = pd.to_datetime(df['date'], unit='s')

            # Store the full dataset in session state
            st.session_state.full_df = df
            st.session_state.comments_df = comments_df

    if st.session_state.full_df is not None:
        # Allow user to select columns after data is loaded
        all_columns = st.session_state.full_df.columns.tolist()
        selected_columns = st.multiselect("Select columns to display and save", all_columns, default=all_columns, key='selected_columns')

        st.subheader("Posts")
        st.write(st.session_state.full_df[selected_columns])

        csv = st.session_state.full_df[selected_columns].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Posts CSV",
            data=csv,
            file_name="vk_posts.csv",
            mime="text/csv",
        )

        if include_comments and not st.session_state.comments_df.empty:
            st.subheader("Comments")
            display_option = st.radio("Choose display option", ["Table view", "Post view"])
            
            if display_option == "Table view":
                st.write(st.session_state.comments_df)

                comments_csv = st.session_state.comments_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Comments CSV",
                    data=comments_csv,
                    file_name="vk_comments.csv",
                    mime="text/csv",
                )
            else:  # Post view
                # Prepare data for post view
                posts_with_comments = st.session_state.full_df.copy()
                posts_with_comments['comments'] = posts_with_comments['id'].apply(
                    lambda x: st.session_state.comments_df[st.session_state.comments_df['post_id'] == x].to_dict('records')
                )
                posts_with_comments['comment_count'] = posts_with_comments['comments'].apply(len)

                # Sorting options
                sort_option = st.selectbox("Sort posts by", ["Most commented", "Newest", "Oldest"])
                if sort_option == "Most commented":
                    posts_with_comments = posts_with_comments.sort_values('comment_count', ascending=False)
                elif sort_option == "Newest":
                    posts_with_comments = posts_with_comments.sort_values('date', ascending=False)
                else:  # Oldest
                    posts_with_comments = posts_with_comments.sort_values('date')

                # Number of top posts to display
                top_n = st.slider("Number of top posts to display", min_value=1, max_value=len(posts_with_comments), value=5)

                # Display posts with comments
                for _, post in posts_with_comments.head(top_n).iterrows():
                    display_post_with_comments(post, post['comments'])

    elif st.session_state.full_df is None and 'Start Parsing' in st.session_state.button_clicked:
        st.warning("No data found for the given parameters.")

if __name__ == "__main__":
    main()