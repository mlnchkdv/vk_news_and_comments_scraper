import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
import vk_api
from dotenv import load_dotenv
import os
import asyncio
import aiohttp

# Load environment variables
load_dotenv()

async def fetch_vk_data(api_key, keyword, start_date, end_date, include_comments, progress_bar):
    vk_session = vk_api.VkApi(token=api_key)
    vk = vk_session.get_api()
    
    all_posts = []
    current_date = start_date
    
    async with aiohttp.ClientSession() as session:
        while current_date <= end_date:
            try:
                # Fetch posts for the current date
                posts = await asyncio.to_thread(
                    vk.wall.search,
                    query=keyword,
                    count=100,
                    start_time=int(current_date.timestamp()),
                    end_time=int((current_date + timedelta(days=1)).timestamp())
                )
                
                for post in posts['items']:
                    post_data = {
                        'id': post['id'],
                        'text': post['text'],
                        'likes': post['likes']['count'],
                        'views': post.get('views', {}).get('count', 0),
                        'reposts': post['reposts']['count'],
                        'date': datetime.fromtimestamp(post['date']),
                    }
                    
                    if include_comments:
                        comments = await asyncio.to_thread(
                            vk.wall.getComments,
                            post_id=post['id'],
                            count=100
                        )
                        post_data['comments'] = comments['items']
                        post_data['comment_count'] = comments['count']
                    
                    all_posts.append(post_data)
                
                current_date += timedelta(days=1)
                progress_bar.progress((current_date - start_date).days / (end_date - start_date).days)
                await asyncio.sleep(0.5)  # Add delay to avoid rate limiting
            
            except vk_api.exceptions.ApiError as e:
                st.error(f"VK API Error: {str(e)}")
                break
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                break
    
    return pd.DataFrame(all_posts)

def main():
    st.title("VK News and Comments Fetcher")

    api_key = os.getenv("VK_API_KEY") or st.text_input("Enter your VK API key", type="password")
    keyword = st.text_input("Enter keyword or expression")
    start_date = st.date_input("Start date")
    end_date = st.date_input("End date")
    include_comments = st.checkbox("Include comments")

    if st.button("Start Fetching"):
        if (end_date - start_date).days < 1:
            st.error("The minimum period should be at least 1 day.")
        else:
            progress_bar = st.progress(0)
            data = asyncio.run(fetch_vk_data(api_key, keyword, start_date, end_date, include_comments, progress_bar))
            
            if not data.empty:
                st.dataframe(data)
                
                csv = data.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name="vk_data.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No data was fetched. Please check your inputs and try again.")

if __name__ == "__main__":
    main()