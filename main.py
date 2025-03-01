import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import pytz
from dateutil import parser
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Функция для проверки валидности API ключа
def check_api_key(api_key):
    url = f"https://api.vk.com/method/users.get?access_token={api_key}&v=5.131"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'error' in data and data['error']['error_code'] == 5:
            return False
    return True

# Функция для выполнения запроса к API VK
def execute_query(api_key, owner_id, query, start_time, end_time):
    url = "https://api.vk.com/method/wall.search"
    params = {
        "access_token": api_key,
        "v": "5.131",
        "owner_id": owner_id,
        "query": query,
        "count": 100,
        "start_time": start_time,
        "end_time": end_time
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'error' in data:
            if data['error']['error_code'] == 6:
                return "rate_limit"
            elif data['error']['error_code'] == 5:
                return "invalid_key"
        return data.get('response', {}).get('items', [])
    return []

# Функция для получения комментариев к посту
def get_comments(api_key, owner_id, post_id):
    url = "https://api.vk.com/method/wall.getComments"
    params = {
        "access_token": api_key,
        "v": "5.131",
        "owner_id": owner_id,
        "post_id": post_id,
        "count": 100,
        "sort": "asc",
        "preview_length": 0
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'error' in data:
            if data['error']['error_code'] == 6:
                return "rate_limit"
            elif data['error']['error_code'] == 5:
                return "invalid_key"
        return data.get('response', {}).get('items', [])
    return []

# Функция для обработки одного временного интервала
def process_time_interval(api_keys, owner_id, query, start_time, end_time, search_mode):
    while True:
        api_key = random.choice(api_keys)
        posts = execute_query(api_key, owner_id, query, start_time, end_time)
        
        if posts == "rate_limit":
            time.sleep(1)  # Подождем секунду перед следующей попыткой
            continue
        elif posts == "invalid_key":
            api_keys.remove(api_key)
            if not api_keys:
                st.error("Все API ключи недействительны. Пожалуйста, добавьте новые ключи.")
                return []
            continue
        
        filtered_posts = []
        for post in posts:
            if 'text' in post:
                if search_mode == 'exact' and re.search(r'\b{}\b'.format(re.escape(query)), post['text'], re.IGNORECASE):
                    filtered_posts.append(post)
                elif search_mode == 'partial' and query.lower() in post['text'].lower():
                    filtered_posts.append(post)
        
        return filtered_posts

# Основная функция для поиска новостей
def search_news(api_keys, owner_id, query, start_date, end_date, search_mode, time_step):
    moscow_tz = pytz.timezone('Europe/Moscow')
    start_time = int(start_date.replace(tzinfo=moscow_tz).timestamp())
    end_time = int(end_date.replace(tzinfo=moscow_tz).timestamp())
    
    all_posts = []
    total_intervals = (end_time - start_time) // time_step
    
    with st.progress(0) as progress_bar:
        start_time_progress = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_interval = {executor.submit(process_time_interval, api_keys, owner_id, query, 
                                                  interval_start, min(interval_start + time_step, end_time),
                                                  search_mode): interval_start 
                                  for interval_start in range(start_time, end_time, time_step)}
            
            for i, future in enumerate(as_completed(future_to_interval)):
                all_posts.extend(future.result())
                progress = (i + 1) / total_intervals
                progress_bar.progress(progress)
                
                elapsed_time = time.time() - start_time_progress
                estimated_total_time = elapsed_time / progress if progress > 0 else 0
                remaining_time = estimated_total_time - elapsed_time
                
                st.write(f"Прогресс: {progress:.2%} | "
                         f"Прошло времени: {timedelta(seconds=int(elapsed_time))} | "
                         f"Осталось примерно: {timedelta(seconds=int(remaining_time))}")

    return all_posts

# Функция для форматирования даты и времени
def format_datetime(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

# Streamlit интерфейс
st.title('VK News Scraper')

# Многоязычная поддержка
language = st.selectbox('Выберите язык / Select language', ['Русский', 'English'])

if language == 'Русский':
    instructions = """
    Инструкции по получению API ключа VK:
    1. Перейдите на страницу https://vk.com/apps?act=manage
    2. Нажмите "Создать приложение"
    3. Выберите тип приложения "Standalone"
    4. После создания приложения перейдите в его настройки
    5. Скопируйте "Сервисный ключ доступа" и вставьте его ниже
    """
    owner_id_label = 'ID владельца страницы или группы (используйте "-" для групп)'
    query_label = 'Поисковый запрос'
    start_date_label = 'Начальная дата'
    end_date_label = 'Конечная дата'
    search_mode_label = 'Режим поиска'
    time_step_label = 'Шаг времени (в секундах)'
    search_button = 'Поиск'
    exact_match = 'Точное совпадение'
    partial_match = 'Частичное совпадение'
else:
    instructions = """
    Instructions for obtaining VK API key:
    1. Go to https://vk.com/apps?act=manage
    2. Click "Create application"
    3. Choose application type "Standalone"
    4. After creating the application, go to its settings
    5. Copy the "Service access key" and paste it below
    """
    owner_id_label = 'Owner ID (use "-" for groups)'
    query_label = 'Search query'
    start_date_label = 'Start date'
    end_date_label = 'End date'
    search_mode_label = 'Search mode'
    time_step_label = 'Time step (in seconds)'
    search_button = 'Search'
    exact_match = 'Exact match'
    partial_match = 'Partial match'

st.write(instructions)

# Ввод API ключей
api_keys_input = st.text_area("Введите API ключи (по одному на строку):")
api_keys = [key.strip() for key in api_keys_input.split('\n') if key.strip()]

# Проверка API ключей
valid_api_keys = [key for key in api_keys if check_api_key(key)]

if not valid_api_keys:
    st.error("Нет действительных API ключей. Пожалуйста, проверьте введенные ключи.")
else:
    st.success(f"Найдено {len(valid_api_keys)} действительных API ключей.")

    owner_id = st.text_input(owner_id_label)
    query = st.text_input(query_label)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(start_date_label)
        start_time = st.time_input(f"{start_date_label} (время)")
    with col2:
        end_date = st.date_input(end_date_label)
        end_time = st.time_input(f"{end_date_label} (время)")
    
    search_mode = st.radio(search_mode_label, [exact_match, partial_match])
    time_step = st.number_input(time_step_label, value=86400, step=3600)

    if st.button(search_button):
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        
        search_mode = 'exact' if search_mode == exact_match else 'partial'
        
        posts = search_news(valid_api_keys, owner_id, query, start_datetime, end_datetime, search_mode, time_step)
        
        if posts:
            df = pd.DataFrame(posts)
            df['date'] = df['date'].apply(format_datetime)
            df = df[['id', 'date', 'text', 'likes', 'reposts', 'views']]
            df['likes'] = df['likes'].apply(lambda x: x['count'] if isinstance(x, dict) else 0)
            df['reposts'] = df['reposts'].apply(lambda x: x['count'] if isinstance(x, dict) else 0)
            df['views'] = df['views'].apply(lambda x: x['count'] if isinstance(x, dict) else 0)
            
            st.write(f"Найдено постов: {len(df)}")
            st.dataframe(df)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Скачать как CSV",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )
        else:
            st.write("Посты не найдены.")

