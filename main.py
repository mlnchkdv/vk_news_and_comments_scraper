import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import pytz
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Constants
MAX_POSTS_PER_REQUEST = 100
DEFAULT_PAUSE = 5
MAX_PAUSE = 10

# Function to check if the API key is valid
def check_api_key(api_key):
    try:
        response = requests.get(f"https://api.vk.com/method/users.get?access_token={api_key}&v=5.131")
        data = response.json()
        if 'error' in data:
            if data['error']['error_code'] == 5:
                return False, "Invalid API key"
            elif data['error']['error_code'] == 6:
                return False, "Too many requests. Please try again later or use multiple API keys."
        return True, "API key is valid"
    except Exception as e:
        return False, str(e)

# Function to execute VK API query
def execute_query(api_key, owner_id, query, start_time, end_time, count=MAX_POSTS_PER_REQUEST, offset=0):
    params = {
        'owner_id': owner_id,
        'query': query,
        'count': count,
        'offset': offset,
        'start_time': start_time,
        'end_time': end_time,
        'access_token': api_key,
        'v': '5.131'
    }
    response = requests.get('https://api.vk.com/method/wall.search', params=params)
    return response.json()

# Function to parse posts
def parse_posts(api_keys, owner_id, query, start_time, end_time, search_mode, time_step, pause):
    all_posts = []
    current_time = start_time
    total_api_keys = len(api_keys)
    current_api_key_index = 0

    with st.spinner('Parsing posts...'):
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_parsing_time = time.time()

        while current_time < end_time:
            next_time = min(current_time + time_step, end_time)
            offset = 0
            
            while True:
                api_key = api_keys[current_api_key_index]
                current_api_key_index = (current_api_key_index + 1) % total_api_keys

                response = execute_query(api_key, owner_id, query, current_time, next_time, offset=offset)
                
                if 'error' in response:
                    if response['error']['error_code'] == 6:
                        st.warning(f"API key {api_key[-4:]} is rate limited. Switching to next key.")
                        continue
                    else:
                        st.error(f"Error: {response['error']['error_msg']}")
                        return []

                items = response.get('response', {}).get('items', [])
                
                all_posts.extend(items)
                
                if len(items) < MAX_POSTS_PER_REQUEST:
                    break
                
                offset += MAX_POSTS_PER_REQUEST
                
                time.sleep(pause)

            progress = (current_time - start_time) / (end_time - start_time)
            progress_bar.progress(progress)
            
            elapsed_time = time.time() - start_parsing_time
            estimated_total_time = elapsed_time / progress if progress > 0 else 0
            remaining_time = estimated_total_time - elapsed_time
            
            status_text.text(f"Parsed {len(all_posts)} posts. "
                             f"Elapsed time: {elapsed_time:.2f}s. "
                             f"Estimated time remaining: {remaining_time:.2f}s.")

            current_time = next_time

    filtered_posts = []
    for post in all_posts:
        post_text = post.get('text', '').lower()
        if search_mode == 'Exact phrase':
            if re.search(r'\b' + re.escape(query.lower()) + r'\b', post_text):
                filtered_posts.append(post)
        else:  # Partial match
            if query.lower() in post_text:
                filtered_posts.append(post)

    return filtered_posts

# Main Streamlit app
def main():
    st.title("VK News Scraper")

    # Instructions
    with st.expander("Instructions", expanded=False):
        st.markdown("""
        1. Enter your VK API key(s). You can obtain it from [vk.com/dev](https://vk.com/dev).
        2. Enter the VK community ID (e.g., -1234567 for public pages).
        3. Enter your search query.
        4. Select the date range for your search.
        5. Choose the search mode (Exact phrase or Partial match).
        6. Set the time step for parsing (in hours).
        7. Set the pause between requests (in seconds).
        8. Click "Start Parsing" to begin.

        Note: Using multiple API keys can help distribute the load and reduce the risk of being rate limited.
        """)

    # Technical details and tips
    with st.expander("Technical Details and Tips", expanded=False):
        st.markdown("""
        ### Technical Details
        - This application uses the VK API to fetch posts from VK communities.
        - It supports multi-threading for faster processing when using multiple API keys.
        - The app uses Streamlit for the user interface and pandas for data manipulation.

        ### Tips for Optimal Use
        1. **API Key Management**: 
           - Use multiple API keys to distribute the load and avoid rate limiting.
           - If you're frequently hitting rate limits, consider increasing the pause between requests.

        2. **Time Step Optimization**:
           - For recent posts or small date ranges, a smaller time step (e.g., 1-6 hours) works well.
           - For older posts or large date ranges, use a larger time step (e.g., 24-48 hours) to speed up the process.

        3. **Handling Large Periods**:
           - Break down large periods into smaller chunks (e.g., monthly or quarterly).
           - Use a larger time step for older data and a smaller step for more recent data.
           - Consider running the scraper multiple times with different date ranges and combining the results.

        4. **Search Query Optimization**:
           - Use specific and unique phrases for better results.
           - For broad topics, consider running multiple searches with related keywords.

        5. **Performance Considerations**:
           - The more API keys you use, the faster the scraping process, but be mindful of VK's rate limits.
           - If you're scraping a large amount of data, consider running the process during off-peak hours.

        Remember to always comply with VK's terms of service and respect the platform's usage limits.
        """)

    # API key input
    api_keys_input = st.text_area("Enter VK API key(s) (one per line):", height=100)
    api_keys = [key.strip() for key in api_keys_input.split('\n') if key.strip()]

    if not api_keys:
        st.warning("Please enter at least one API key.")
        return

    # Validate API keys
    invalid_keys = []
    for api_key in api_keys:
        is_valid, message = check_api_key(api_key)
        if not is_valid:
            invalid_keys.append((api_key, message))

    if invalid_keys:
        for key, message in invalid_keys:
            st.error(f"Invalid API key ({key[-4:]}): {message}")
        return

    owner_id = st.text_input("Enter VK community ID (e.g., -1234567 for public pages):")
    query = st.text_input("Enter search query:")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start date:")
        start_time = st.time_input("Start time:", value=datetime.min.time())
    with col2:
        end_date = st.date_input("End date:")
        end_time = st.time_input("End time:", value=datetime.max.time())

    search_mode = st.selectbox("Search mode:", ["Partial match", "Exact phrase"])
    time_step = st.number_input("Time step for parsing (hours):", min_value=1, max_value=168, value=24)
    pause = st.slider("Pause between requests (seconds):", min_value=1, max_value=MAX_PAUSE, value=DEFAULT_PAUSE)

    if st.button("Start Parsing"):
        if not all([api_keys, owner_id, query, start_date, end_date]):
            st.warning("Please fill in all required fields.")
            return

        start_datetime = datetime.combine(start_date, start_time).replace(tzinfo=pytz.UTC)
        end_datetime = datetime.combine(end_date, end_time).replace(tzinfo=pytz.UTC)

        posts = parse_posts(api_keys, owner_id, query, int(start_datetime.timestamp()), int(end_datetime.timestamp()),
                            search_mode, timedelta(hours=time_step), pause)

        if posts:
            df = pd.DataFrame(posts)
            df['date'] = pd.to_datetime(df['date'], unit='s')
            df = df.sort_values('date', ascending=False)

            st.write(f"Total posts found: {len(posts)}")
            st.dataframe(df[['date', 'text', 'likes', 'reposts', 'views']])

            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )
        else:
            st.info("No posts found matching the criteria.")

if __name__ == "__main__":
    main()