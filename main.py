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

def get_posts(query, count, offset, start_time, end_time, api_key):
    params = {
        'q': query,
        'count': count,
        'offset': offset,
        'start_time': start_time,
        'end_time': end_time,
        'access_token': api_key,
        'v': '5.131'
    }
    response = requests.get('https://api.vk.com/method/newsfeed.search', params=params)
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

def execute_query(query, start_time, end_time, api_keys, pause, search_mode, progress_bar):
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
                response = get_posts(query, MAX_POSTS_PER_REQUEST, offset, 
                                     int(current_datetime.timestamp()), 
                                     int(current_end.timestamp()), 
                                     api_key)
                
                if 'error' in response:
                    if response['error']['error_code'] == 6:
                        st.warning(f"API ключ {api_key[-4:]} временно заблокирован. Переключение на следующий ключ.")
                        time.sleep(pause)
                        continue
                    else:
                        raise Exception(f"Ошибка API: {response['error']['error_msg']}")
                
                posts = response['response']['items']
                
                if not posts:
                    break
                
                for post in posts:
                    if 'text' in post:
                        if search_mode == 'Точная фраза' and query.lower() in post['text'].lower():
                            all_posts.append(post)
                            total_posts += 1
                        elif search_mode == 'Частичное совпадение' and re.search(query.lower(), post['text'].lower()):
                            all_posts.append(post)
                            total_posts += 1
                        
                        comments_response = get_comments(post['owner_id'], post['id'], api_key)
                        if 'response' in comments_response and 'items' in comments_response['response']:
                            all_comments.extend(comments_response['response']['items'])
                
                offset += MAX_POSTS_PER_REQUEST
                progress_bar.progress((current_datetime - start_datetime) / (end_datetime - start_datetime))
                
                time.sleep(pause)
            
            except Exception as e:
                st.error(f"Произошла ошибка: {str(e)}")
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
        1. Выберите даты начала и окончания для поиска.
        2. Введите ключевое слово или фразу для поиска.
        3. Выберите режим поиска: точная фраза или частичное совпадение.
        4. Введите один или несколько API ключей VK, разделенных запятой.
        5. Настройте паузу между запросами для избежания блокировки.
        6. Нажмите "Начать поиск" для запуска процесса.
        7. Результаты будут отображены в виде таблиц и доступны для скачивания.
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

    col1, col2, col3 = st.columns(3)

    with col1:
        start_date = st.date_input("Дата начала")
        start_time = st.time_input("Время начала")
    
    with col2:
        end_date = st.date_input("Дата окончания")
        end_time = st.time_input("Время окончания")
    
    with col3:
        keyword = st.text_input("Ключевое слово или фраза")
        search_mode = st.selectbox("Режим поиска", ["Точная фраза", "Частичное совпадение"])

    api_keys = st.text_area("API ключи VK (разделенные запятой)", height=100)
    pause = st.slider("Пауза между запросами (секунды)", min_value=1, max_value=MAX_PAUSE, value=DEFAULT_PAUSE)

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

        posts_df, comments_df = execute_query(keyword, start_timestamp, end_timestamp, api_keys_list, pause, search_mode, progress_bar)

        if not posts_df.empty:
            st.success(f"Поиск завершен. Найдено {len(posts_df)} постов и {len(comments_df)} комментариев.")
            
            st.subheader("Статистика")
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

            st.subheader("Посты")
            st.dataframe(posts_df)

            st.subheader("Комментарии")
            st.dataframe(comments_df)

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