import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import pytz
from dateutil import parser
import re
from tqdm import tqdm
import numpy as np

# Функция для выполнения запроса к API VK
def execute_query(method, params, api_keys):
    base_url = "https://api.vk.com/method/"
    api_key = api_keys[0] if isinstance(api_keys, list) else api_keys
    params['access_token'] = api_key
    params['v'] = '5.131'
    
    response = requests.get(base_url + method, params=params)
    data = response.json()
    
    if 'error' in data:
        if data['error']['error_code'] == 6:  # Too many requests per second
            time.sleep(0.5)  # Подождем полсекунды
            return execute_query(method, params, api_keys[1:] if isinstance(api_keys, list) else api_keys)
        elif data['error']['error_code'] == 5:  # User authorization failed
            st.error(f"Ошибка авторизации для ключа API: {api_key}. Возможно, ключ был заблокирован.")
            return None
    
    return data

# Функция для получения постов
def get_posts(owner_id, start_time, end_time, api_keys, query, search_mode, time_step):
    all_posts = []
    current_time = start_time
    
    with st.spinner('Идет поиск постов...'):
        progress_bar = st.progress(0)
        start_process_time = time.time()
        
        while current_time < end_time:
            next_time = min(current_time + time_step, end_time)
            
            params = {
                'owner_id': owner_id,
                'count': 100,
                'start_time': int(current_time.timestamp()),
                'end_time': int(next_time.timestamp())
            }
            
            response = execute_query('wall.get', params, api_keys)
            if response is None:
                return None
            
            posts = response.get('response', {}).get('items', [])
            
            if search_mode == 'Точная фраза':
                filtered_posts = [post for post in posts if 'text' in post and re.search(r'\b' + re.escape(query) + r'\b', post['text'], re.IGNORECASE)]
            else:  # 'Любое вхождение'
                filtered_posts = [post for post in posts if 'text' in post and query.lower() in post['text'].lower()]
            
            all_posts.extend(filtered_posts)
            
            current_time = next_time
            progress = (current_time - start_time) / (end_time - start_time)
            progress_bar.progress(progress)
            
            elapsed_time = time.time() - start_process_time
            estimated_total_time = elapsed_time / progress if progress > 0 else 0
            remaining_time = estimated_total_time - elapsed_time
            
            st.text(f"Прогресс: {progress:.2%}")
            st.text(f"Прошло времени: {timedelta(seconds=int(elapsed_time))}")
            st.text(f"Осталось примерно: {timedelta(seconds=int(remaining_time))}")
            st.text(f"Найдено постов: {len(all_posts)}")
    
    return all_posts

# Функция для получения комментариев
def get_comments(owner_id, post_id, api_keys):
    all_comments = []
    offset = 0
    
    while True:
        params = {
            'owner_id': owner_id,
            'post_id': post_id,
            'count': 100,
            'offset': offset,
            'sort': 'asc',
            'extended': 1
        }
        
        response = execute_query('wall.getComments', params, api_keys)
        if response is None:
            return None
        
        comments = response.get('response', {}).get('items', [])
        if not comments:
            break
        
        all_comments.extend(comments)
        offset += 100
    
    return all_comments

def main():
    st.title('VK News Scraper')
    
    language = st.sidebar.selectbox('Выберите язык / Select language', ['Русский', 'English'])
    
    if language == 'Русский':
        instructions = """
        Инструкции:
        1. Введите ID сообщества VK (например, -1 для группы /public1).
        2. Введите ключевое слово или фразу для поиска.
        3. Выберите режим поиска (точная фраза или любое вхождение).
        4. Укажите диапазон дат для поиска.
        5. Введите один или несколько ключей API VK (разделенные запятыми).
        6. Нажмите "Начать поиск".
        
        Для получения ключа API VK:
        1. Перейдите на https://vk.com/dev
        2. Нажмите "Мои приложения"
        3. Создайте новое приложение (если у вас его еще нет)
        4. Перейдите в настройки приложения
        5. Скопируйте "Сервисный ключ доступа"
        """
    else:
        instructions = """
        Instructions:
        1. Enter the VK community ID (e.g., -1 for /public1 group).
        2. Enter a keyword or phrase to search for.
        3. Choose the search mode (exact phrase or any occurrence).
        4. Specify the date range for the search.
        5. Enter one or more VK API keys (separated by commas).
        6. Click "Start Search".
        
        To obtain a VK API key:
        1. Go to https://vk.com/dev
        2. Click on "My Apps"
        3. Create a new application (if you don't have one)
        4. Go to the application settings
        5. Copy the "Service access key"
        """
    
    st.sidebar.info(instructions)
    
    owner_id = st.text_input('ID сообщества VK / VK Community ID', '-1')
    query = st.text_input('Ключевое слово или фраза / Keyword or phrase', 'Пример')
    search_mode = st.radio('Режим поиска / Search mode', ['Точная фраза / Exact phrase', 'Любое вхождение / Any occurrence'])
    search_mode = search_mode.split(' / ')[0]  # Берем только русскую часть
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input('Начальная дата / Start date', datetime.now() - timedelta(days=7))
    with col2:
        end_date = st.date_input('Конечная дата / End date', datetime.now())
    
    col3, col4 = st.columns(2)
    with col3:
        start_time = st.time_input('Начальное время / Start time', datetime.min.time())
    with col4:
        end_time = st.time_input('Конечное время / End time', datetime.max.time())
    
    api_keys = st.text_input('Ключ(и) API VK (разделенные запятыми) / VK API key(s) (comma-separated)', '')
    api_keys = [key.strip() for key in api_keys.split(',') if key.strip()]
    
    time_step = st.slider('Шаг времени (в часах) / Time step (in hours)', 1, 24, 6)
    time_step = timedelta(hours=time_step)
    
    if st.button('Начать поиск / Start Search'):
        if not api_keys:
            st.error('Пожалуйста, введите хотя бы один ключ API / Please enter at least one API key')
            return
        
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        
        posts = get_posts(owner_id, start_datetime, end_datetime, api_keys, query, search_mode, time_step)
        
        if posts is None:
            st.error('Произошла ошибка при выполнении запроса. Пожалуйста, проверьте ваш ключ API и попробуйте снова.')
            return
        
        if not posts:
            st.warning('Посты не найдены / No posts found')
            return
        
        data = []
        for post in posts:
            post_id = post['id']
            post_text = post.get('text', '')
            post_date = datetime.fromtimestamp(post['date'], pytz.UTC)
            likes = post.get('likes', {}).get('count', 0)
            reposts = post.get('reposts', {}).get('count', 0)
            views = post.get('views', {}).get('count', 0)
            
            comments = get_comments(owner_id, post_id, api_keys)
            if comments is None:
                st.error('Произошла ошибка при получении комментариев. Пожалуйста, проверьте ваш ключ API и попробуйте снова.')
                return
            
            for comment in comments:
                comment_id = comment['id']
                comment_text = comment.get('text', '')
                comment_date = datetime.fromtimestamp(comment['date'], pytz.UTC)
                comment_likes = comment.get('likes', {}).get('count', 0)
                
                data.append({
                    'Post ID': post_id,
                    'Post Text': post_text,
                    'Post Date': post_date,
                    'Post Likes': likes,
                    'Post Reposts': reposts,
                    'Post Views': views,
                    'Comment ID': comment_id,
                    'Comment Text': comment_text,
                    'Comment Date': comment_date,
                    'Comment Likes': comment_likes
                })
        
        df = pd.DataFrame(data)
        st.write(df)
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Скачать данные как CSV / Download data as CSV",
            data=csv,
            file_name="vk_news_data.csv",
            mime="text/csv",
        )

if __name__ == "__main__":
    main()
