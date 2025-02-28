import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime, timedelta
import pytz
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Constants
MAX_POSTS_PER_REQUEST = 100
DEFAULT_PAUSE = 5
MAX_PAUSE = 10

def get_posts(owner_id, count, offset, start_time, end_time, api_key):
    params = {
        'owner_id': owner_id,
        'count': count,
        'offset': offset,
        'start_time': start_time,
        'end_time': end_time,
        'access_token': api_key,
        'v': '5.131'
    }
    response = requests.get('https://api.vk.com/method/wall.get', params=params)
    return response.json()

def get_comments(owner_id, post_id, api_key):
    params = {
        'owner_id': owner_id,
        'post_id': post_id,
        'count': 100,
        'sort': 'asc',
        'access_token': api_key,
        'v': '5.131'
    }
    response = requests.get('https://api.vk.com/method/wall.getComments', params=params)
    return response.json()

def execute_query(owner_id, start_time, end_time, keyword, api_keys, pause, search_mode, progress_bar):
    all_posts = []
    all_comments = []
    total_posts = 0
    current_api_key_index = 0
    
    start_datetime = datetime.fromtimestamp(start_time, pytz.UTC)
    end_datetime = datetime.fromtimestamp(end_time, pytz.UTC)
    current_datetime = start_datetime
    
    while current_datetime <= end_datetime:
        current_end = min(current_datetime + timedelta(days=1), end_datetime)
        offset = 0
        
        while True:
            api_key = api_keys[current_api_key_index]
            current_api_key_index = (current_api_key_index + 1) % len(api_keys)
            
            try:
                response = get_posts(owner_id, MAX_POSTS_PER_REQUEST, offset, 
                                     int(current_datetime.timestamp()), 
                                     int(current_end.timestamp()), 
                                     api_key)
                
                if 'error' in response:
                    if response['error']['error_code'] == 6:
                        st.warning(f"API key {api_key[-4:]} is temporarily banned. Switching to next key.")
                        time.sleep(pause)
                        continue
                    else:
                        raise Exception(f"API Error: {response['error']['error_msg']}")
                
                posts = response['response']['items']
                
                if not posts:
                    break
                
                for post in posts:
                    if 'text' in post:
                        if search_mode == 'Точная фраза' and keyword.lower() in post['text'].lower():
                            all_posts.append(post)
                            total_posts += 1
                        elif search_mode == 'Частичное совпадение' and re.search(keyword.lower(), post['text'].lower()):
                            all_posts.append(post)
                            total_posts += 1
                        
                        comments_response = get_comments(owner_id, post['id'], api_key)
                        if 'response' in comments_response and 'items' in comments_response['response']:
                            all_comments.extend(comments_response['response']['items'])
                
                offset += MAX_POSTS_PER_REQUEST
                progress_bar.progress((current_datetime - start_datetime) / (end_datetime - start_datetime))
                
                time.sleep(pause)
            
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                return pd.DataFrame(), pd.DataFrame()
        
        current_datetime = current_end
    
    posts_df = pd.DataFrame(all_posts)
    comments_df = pd.DataFrame(all_comments)
    
    return posts_df, comments_df

def main():
    st.set_page_config(page_title="VK News Scraper", layout="wide")
    st.title("VK News Scraper")

    # Instructions
    with st.expander("Инструкция"):
        st.markdown("""
        1. Введите ID сообщества VK (например, -1 для публичной страницы VK).
        2. Выберите даты начала и окончания для поиска.
        3. Введите ключевое слово или фразу для поиска.
        4. Выберите режим поиска: точная фраза или частичное совпадение.
        5. Введите один или несколько API ключей VK, разделенных запятой.
        6. Настройте паузу между запросами для избежания блокировки.
        7. Нажмите "Начать поиск" для запуска процесса.
        8. Результаты будут отображены в отдельных вкладках.
        """)

    # Technical details and tips
    with st.expander("Технические особенности и советы"):
        st.markdown("""
        - Приложение использует API VK для получения постов и комментариев.
        - Многопоточность применяется для ускорения процесса парсинга.
        - Рекомендуемая пауза между запросами: 5-10 секунд для избежания блокировки.
        - При использовании нескольких API ключей нагрузка распределяется равномерно.
        - Для больших периодов рекомендуется:
          1. Разбить период на меньшие интервалы (например, по месяцам).
          2. Использовать несколько API ключей для распределения нагрузки.
          3. Увеличить паузу между запросами до 7-10 секунд.
          4. Запускать парсинг в нерабочее время, когда нагрузка на API VK меньше.
        - При блокировке API ключа приложение автоматически переключится на следующий доступный ключ.
        """)

    col1, col2 = st.columns(2)

    with col1:
        owner_id = st.text_input("ID сообщества VK", value="-1")
        start_date = st.date_input("Дата начала")
        end_date = st.date_input("Дата окончания")
        keyword = st.text_input("Ключевое слово или фраза")
        search_mode = st.selectbox("Режим поиска", ["Точная фраза", "Частичное совпадение"])

    with col2:
        api_keys = st.text_area("API ключи VK (разделенные запятой)", height=100)
        pause = st.slider("Пауза между запросами (секунды)", min_value=1, max_value=MAX_PAUSE, value=DEFAULT_PAUSE)
        start_time = st.time_input("Время начала")
        end_time = st.time_input("Время окончания")

    if st.button("Начать поиск"):
        api_keys_list = [key.strip() for key in api_keys.split(',') if key.strip()]
        if not api_keys_list:
            st.error("Пожалуйста, введите хотя бы один API ключ.")
            return

        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        
        start_timestamp = int(start_datetime.timestamp())
        end_timestamp = int(end_datetime.timestamp())

        progress_bar = st.progress(0)
        start_time = time.time()

        posts_df, comments_df = execute_query(owner_id, start_timestamp, end_timestamp, keyword, api_keys_list, pause, search_mode, progress_bar)

        if not posts_df.empty:
            st.success(f"Поиск завершен. Найдено {len(posts_df)} постов и {len(comments_df)} комментариев.")
            
            # Create tabs for results
            tab1, tab2, tab3 = st.tabs(["Статистика", "Посты", "Комментарии"])
            
            with tab1:
                st.subheader("Ключевая информация и статистика")
                st.write(f"Общее количество найденных постов: {len(posts_df)}")
                st.write(f"Общее количество комментариев: {len(comments_df)}")
                st.write(f"Период поиска: с {start_datetime} по {end_datetime}")
                st.write(f"Ключевое слово: {keyword}")
                st.write(f"Режим поиска: {search_mode}")
                
                if not posts_df.empty:
                    st.write("Статистика по постам:")
                    st.write(f"Среднее количество лайков: {posts_df['likes'].apply(lambda x: x['count'] if isinstance(x, dict) else 0).mean():.2f}")
                    st.write(f"Среднее количество репостов: {posts_df['reposts'].apply(lambda x: x['count'] if isinstance(x, dict) else 0).mean():.2f}")
                    st.write(f"Среднее количество просмотров: {posts_df['views'].apply(lambda x: x['count'] if isinstance(x, dict) else 0).mean():.2f}")
                
                if not comments_df.empty:
                    st.write("Статистика по комментариям:")
                    st.write(f"Среднее количество лайков на комментарий: {comments_df['likes'].mean():.2f}")
            
            with tab2:
                st.subheader("Посты")
                if not posts_df.empty:
                    for index, post in posts_df.iterrows():
                        st.write(f"Пост {index + 1}")
                        st.write(f"Текст: {post['text']}")
                        st.write(f"Дата: {datetime.fromtimestamp(post['date'])}")
                        st.write(f"Лайки: {post['likes']['count'] if isinstance(post['likes'], dict) else 0}")
                        st.write(f"Репосты: {post['reposts']['count'] if isinstance(post['reposts'], dict) else 0}")
                        st.write(f"Просмотры: {post['views']['count'] if isinstance(post['views'], dict) else 0}")
                        st.write("---")
                else:
                    st.write("Посты не найдены.")
            
            with tab3:
                st.subheader("Комментарии")
                if not comments_df.empty:
                    for index, comment in comments_df.iterrows():
                        st.write(f"Комментарий {index + 1}")
                        st.write(f"Текст: {comment['text']}")
                        st.write(f"Дата: {datetime.fromtimestamp(comment['date'])}")
                        st.write(f"Лайки: {comment['likes']}")
                        st.write("---")
                else:
                    st.write("Комментарии не найдены.")

            # Save results to CSV
            posts_csv = posts_df.to_csv(index=False).encode('utf-8')
            comments_csv = comments_df.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label="Скачать посты (CSV)",
                data=posts_csv,
                file_name="vk_posts.csv",
                mime="text/csv"
            )
            
            st.download_button(
                label="Скачать комментарии (CSV)",
                data=comments_csv,
                file_name="vk_comments.csv",
                mime="text/csv"
            )
        else:
            st.warning("Посты не найдены.")

if __name__ == "__main__":
    main()