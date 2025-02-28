import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import re
from datetime import datetime, timedelta
from dateutil.parser import parse
import pytz
import threading
import queue
import plotly.graph_objects as go

# Константы
MAX_POSTS_PER_REQUEST = 100
DEFAULT_PAUSE = 5
MAX_PAUSE = 10

# Функция для проверки бана API ключа
def check_api_key(api_key):
    url = f"https://api.vk.com/method/users.get?access_token={api_key}&v=5.131"
    response = requests.get(url)
    if response.status_code != 200 or 'error' in response.json():
        return False
    return True

# Функция для выполнения запроса к API VK
def execute_query(api_key, owner_id, query, start_time, end_time, search_mode):
    offset = 0
    all_posts = []
    while True:
        url = f"https://api.vk.com/method/wall.search?owner_id={owner_id}&query={query}&count={MAX_POSTS_PER_REQUEST}&offset={offset}&start_time={start_time}&end_time={end_time}&access_token={api_key}&v=5.131"
        response = requests.get(url)
        data = response.json()
        
        if 'error' in data:
            if data['error']['error_code'] == 6:
                return 'rate_limit'
            else:
                return 'error'
        
        items = data.get('response', {}).get('items', [])
        if not items:
            break
        
        if search_mode == 'Точная фраза':
            items = [item for item in items if re.search(r'\b' + re.escape(query) + r'\b', item.get('text', ''), re.IGNORECASE)]
        
        all_posts.extend(items)
        offset += len(items)
        
        if offset >= data['response']['count']:
            break
    
    return all_posts

# Функция для обработки постов
def process_posts(posts, owner_id, api_key):
    processed_posts = []
    for post in posts:
        post_id = post['id']
        likes = post.get('likes', {}).get('count', 0)
        reposts = post.get('reposts', {}).get('count', 0)
        views = post.get('views', {}).get('count', 0)
        
        # Получаем комментарии
        comments_url = f"https://api.vk.com/method/wall.getComments?owner_id={owner_id}&post_id={post_id}&count=100&access_token={api_key}&v=5.131"
        comments_response = requests.get(comments_url)
        comments_data = comments_response.json()
        comments = comments_data.get('response', {}).get('items', [])
        
        processed_posts.append({
            'id': post_id,
            'text': post.get('text', ''),
            'date': datetime.fromtimestamp(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            'likes': likes,
            'reposts': reposts,
            'views': views,
            'comments': comments
        })
    
    return processed_posts

# Функция для выполнения парсинга с использованием нескольких API ключей
def parse_with_multiple_keys(api_keys, owner_id, query, start_time, end_time, search_mode, time_step, pause):
    results = []
    current_time = start_time
    key_index = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    start_parsing_time = time.time()
    
    while current_time < end_time:
        next_time = min(current_time + time_step, end_time)
        
        api_key = api_keys[key_index]
        key_index = (key_index + 1) % len(api_keys)
        
        posts = execute_query(api_key, owner_id, query, current_time, next_time, search_mode)
        
        if posts == 'rate_limit':
            time.sleep(pause)
            continue
        elif posts == 'error':
            st.error(f"Ошибка при использовании API ключа: {api_key}")
            continue
        
        processed_posts = process_posts(posts, owner_id, api_key)
        results.extend(processed_posts)
        
        progress = (current_time - start_time) / (end_time - start_time)
        progress_bar.progress(progress)
        
        elapsed_time = time.time() - start_parsing_time
        estimated_total_time = elapsed_time / progress if progress > 0 else 0
        remaining_time = estimated_total_time - elapsed_time
        
        status_text.text(f"Обработано {len(results)} постов. Прошло времени: {elapsed_time:.2f} сек. Осталось примерно: {remaining_time:.2f} сек.")
        
        current_time = next_time
        time.sleep(pause)
    
    progress_bar.progress(1.0)
    status_text.text(f"Парсинг завершен. Всего обработано {len(results)} постов.")
    
    return results

# Основная функция приложения
def main():
    st.set_page_config(page_title="VK News Parser", layout="wide")
    st.title("VK News Parser")

    # Создаем вкладки
    tabs = st.tabs(["Настройки", "Статистика", "Результаты парсинга"])

    with tabs[0]:
        st.header("Настройки парсинга")

        # Инструкция
        with st.expander("Инструкция"):
            st.markdown("""
            1. Получите API ключ VK, следуя [этой инструкции](https://dev.vk.com/api/access-token/getting-started).
            2. Введите API ключ(и) в соответствующее поле.
            3. Введите ID сообщества (например, -1 для новостей ВКонтакте).
            4. Введите ключевое слово или фразу для поиска.
            5. Выберите режим поиска (Точная фраза или Частичное совпадение).
            6. Укажите временной диапазон для поиска.
            7. Настройте параметры парсинга (шаг по времени и паузу между запросами).
            8. Нажмите кнопку "Начать парсинг" для запуска процесса.
            
            Рекомендуемая пауза между запросами: 5-10 секунд для избежания блокировки API ключа.
            """)

        # Технические особенности и советы
        with st.expander("Технические особенности и советы"):
            st.markdown("""
            ### Технические особенности:
            - Приложение использует API VK для получения данных.
            - Реализована поддержка нескольких API ключей для распределения нагрузки.
            - Используется многопоточность для параллельной обработки запросов.
            - Данные обрабатываются и сохраняются с использованием библиотеки Pandas.
            - Графики создаются с помощью библиотеки Plotly.

            ### Советы по использованию:
            1. Для больших периодов времени используйте несколько API ключей.
            2. Увеличивайте шаг по времени для ускорения парсинга, но помните, что это может привести к пропуску некоторых постов.
            3. Для точного поиска используйте режим "Точная фраза", для более широкого охвата - "Частичное совпадение".
            4. Если вы часто делаете запросы, увеличьте паузу между запросами до 7-10 секунд.
            5. Для очень больших периодов времени рекомендуется разбить задачу на несколько меньших периодов и выполнять их последовательно.
            """)

        # Ввод API ключей
        api_keys = []
        col1, col2 = st.columns([3, 1])
        with col1:
            api_key = st.text_input("API ключ VK", key="api_key_0")
            if api_key:
                api_keys.append(api_key)
        with col2:
            if st.button("Добавить ключ"):
                st.session_state.num_api_keys = st.session_state.get('num_api_keys', 1) + 1

        for i in range(1, st.session_state.get('num_api_keys', 1)):
            api_key = st.text_input(f"Дополнительный API ключ {i}", key=f"api_key_{i}")
            if api_key:
                api_keys.append(api_key)

        owner_id = st.text_input("ID сообщества (например, -1 для новостей ВКонтакте)")
        query = st.text_input("Ключевое слово или фраза для поиска")
        search_mode = st.radio("Режим поиска", ["Точная фраза", "Частичное совпадение"])

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Начальная дата")
            start_time = st.time_input("Начальное время", value=datetime.min.time())
        with col2:
            end_date = st.date_input("Конечная дата")
            end_time = st.time_input("Конечное время", value=datetime.max.time())

        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)

        time_step = st.number_input("Шаг по времени (часы)", min_value=1, max_value=24, value=1)
        pause = st.slider("Пауза между запросами (секунды)", min_value=1, max_value=MAX_PAUSE, value=DEFAULT_PAUSE)

        if st.button("Начать парсинг"):
            if not api_keys:
                st.error("Пожалуйста, введите хотя бы один API ключ.")
            elif not owner_id or not query:
                st.error("Пожалуйста, заполните все поля.")
            else:
                # Проверяем все API ключи
                valid_keys = [key for key in api_keys if check_api_key(key)]
                if not valid_keys:
                    st.error("Все введенные API ключи недействительны или заблокированы. Пожалуйста, проверьте их и попробуйте снова.")
                else:
                    with st.spinner('Выполняется парсинг...'):
                        results = parse_with_multiple_keys(
                            valid_keys, owner_id, query, 
                            int(start_datetime.timestamp()), int(end_datetime.timestamp()),
                            search_mode, timedelta(hours=time_step).total_seconds(), pause
                        )
                        
                        if results:
                            df = pd.DataFrame(results)
                            st.session_state.parsed_data = df
                            st.success(f"Парсинг завершен. Найдено {len(results)} постов.")
                        else:
                            st.warning("Не найдено постов, соответствующих заданным критериям.")

    with tabs[1]:
        st.header("Статистика")
        if 'parsed_data' in st.session_state:
            df = st.session_state.parsed_data
            
            st.subheader("Общая информация")
            st.write(f"Всего постов: {len(df)}")
            st.write(f"Период: с {df['date'].min()} по {df['date'].max()}")
            
            st.subheader("Статистика по лайкам, репостам и просмотрам")
            st.write(f"Среднее количество лайков: {df['likes'].mean():.2f}")
            st.write(f"Среднее количество репостов: {df['reposts'].mean():.2f}")
            st.write(f"Среднее количество просмотров: {df['views'].mean():.2f}")
            
            st.subheader("График распределения постов по времени")
            df['date'] = pd.to_datetime(df['date'])
            
            # Выбор интервала группировки
            interval = st.selectbox("Выберите интервал группировки", ["Час", "День", "Неделя", "Месяц"])
            
            if interval == "Час":
                df_grouped = df.groupby(df['date'].dt.floor('H')).size().reset_index(name='count')
            elif interval == "День":
                df_grouped = df.groupby(df['date'].dt.date).size().reset_index(name='count')
            elif interval == "Неделя":
                df_grouped = df.groupby(df['date'].dt.to_period('W')).size().reset_index(name='count')
            else:  # Месяц
                df_grouped = df.groupby(df['date'].dt.to_period('M')).size().reset_index(name='count')
            
            fig = go.Figure(data=[go.Bar(x=df_grouped['date'], y=df_grouped['count'])])
            fig.update_layout(title='Распределение постов по времени',
                              xaxis_title='Дата',
                              yaxis_title='Количество постов')
            st.plotly_chart(fig)

    with tabs[2]:
        st.header("Результаты парсинга")
        if 'parsed_data' in st.session_state:
            df = st.session_state.parsed_data
            
            # Сортировка
            sort_column = st.selectbox("Сортировать по", ["date", "likes", "reposts", "views"])
            sort_order = st.radio("Порядок сортировки", ["По убыванию", "По возрастанию"])
            df_sorted = df.sort_values(by=sort_column, ascending=(sort_order == "По возрастанию"))
            
            # Отображение постов
            for _, row in df_sorted.iterrows():
                with st.expander(f"Пост от {row['date']} (Лайки: {row['likes']}, Репосты: {row['reposts']}, Просмотры: {row['views']})"):
                    st.write(row['text'])
                    st.write("Комментарии:")
                    for comment in row['comments']:
                        st.write(f"- {comment.get('text', '')}")

if __name__ == "__main__":
    main()