import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime, timedelta
import pytz
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.graph_objects as go

# Константы
MAX_POSTS_PER_REQUEST = 200
DEFAULT_TIME_STEP = timedelta(hours=24)
MAX_RETRIES = 3
DEFAULT_PAUSE = 5
MAX_PAUSE = 10

# Функция для выполнения запроса к API VK
def execute_query(api_key, owner_id, query, start_time, end_time, search_mode):
    params = {
        "access_token": api_key,
        "v": "5.131",
        "owner_id": owner_id,
        "query": query,
        "count": MAX_POSTS_PER_REQUEST,
        "start_time": start_time,
        "end_time": end_time,
    }
    
    if search_mode == "Точная фраза":
        params["search_own"] = 1
    
    response = requests.get("https://api.vk.com/method/wall.search", params=params)
    data = response.json()
    
    if "error" in data:
        if data["error"]["error_code"] == 6:
            raise Exception("API key is temporarily banned due to too many requests")
        else:
            raise Exception(f"API Error: {data['error']['error_msg']}")
    
    return data.get("response", {}).get("items", [])

# Функция для получения комментариев к посту
def get_comments(api_key, owner_id, post_id):
    params = {
        "access_token": api_key,
        "v": "5.131",
        "owner_id": owner_id,
        "post_id": post_id,
        "count": 200,
        "sort": "asc",
        "extended": 1,
    }
    
    response = requests.get("https://api.vk.com/method/wall.getComments", params=params)
    data = response.json()
    
    if "error" in data:
        if data["error"]["error_code"] == 6:
            raise Exception("API key is temporarily banned due to too many requests")
        else:
            raise Exception(f"API Error: {data['error']['error_msg']}")
    
    return data.get("response", {}).get("items", [])

# Функция для обработки постов
def process_posts(api_keys, owner_id, queries, start_date, end_date, search_mode, time_step, pause):
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    current_time = start_time
    all_posts = []
    total_requests = 0
    api_key_index = 0

    progress_bar = st.progress(0)
    status_text = st.empty()
    start_process_time = time.time()

    while current_time < end_time:
        next_time = min(current_time + int(time_step.total_seconds()), end_time)
        
        for query in queries:
            try:
                posts = execute_query(api_keys[api_key_index], owner_id, query, current_time, next_time, search_mode)
                all_posts.extend(posts)
                total_requests += 1
                
                # Переключение на следующий API ключ
                api_key_index = (api_key_index + 1) % len(api_keys)
                
                time.sleep(pause)
            except Exception as e:
                st.error(f"Error: {str(e)}")
                if "API key is temporarily banned" in str(e):
                    st.warning(f"API key {api_keys[api_key_index]} is temporarily banned. Switching to the next key.")
                    api_key_index = (api_key_index + 1) % len(api_keys)
                    if api_key_index == 0:
                        st.error("All API keys are banned. Please wait and try again later.")
                        return []
                else:
                    return []

        progress = (current_time - start_time) / (end_time - start_time)
        progress_bar.progress(progress)
        
        elapsed_time = time.time() - start_process_time
        estimated_total_time = elapsed_time / progress if progress > 0 else 0
        remaining_time = estimated_total_time - elapsed_time
        
        status_text.text(f"Прогресс: {progress:.2%}. Прошло времени: {timedelta(seconds=int(elapsed_time))}. Осталось примерно: {timedelta(seconds=int(remaining_time))}. Всего запросов: {total_requests}")
        
        current_time = next_time

    progress_bar.progress(1.0)
    status_text.text(f"Выполнено! Всего запросов: {total_requests}. Общее время: {timedelta(seconds=int(time.time() - start_process_time))}")

    return all_posts

# Функция для форматирования даты и времени
def format_datetime(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

# Функция для создания графика
def create_chart(df, group_by):
    if group_by == 'hour':
        df['grouped_time'] = df['date'].dt.floor('H')
    elif group_by == 'day':
        df['grouped_time'] = df['date'].dt.floor('D')
    elif group_by == 'week':
        df['grouped_time'] = df['date'].dt.to_period('W').apply(lambda r: r.start_time)
    elif group_by == 'month':
        df['grouped_time'] = df['date'].dt.to_period('M').apply(lambda r: r.start_time)
    
    grouped_data = df.groupby('grouped_time').size().reset_index(name='count')
    
    fig = go.Figure(data=[go.Bar(x=grouped_data['grouped_time'], y=grouped_data['count'])])
    fig.update_layout(title='Количество постов по времени',
                      xaxis_title='Время',
                      yaxis_title='Количество постов')
    return fig

# Основная функция приложения
def main():
    st.set_page_config(page_title="VK News Scraper", page_icon="📰", layout="wide")
    st.title("VK News Scraper")

    # Создаем вкладки
    tabs = st.tabs(["Статистика", "Настройки", "Результаты"])

    with tabs[0]:
        st.header("Статистика и график")
        if 'df' in st.session_state and not st.session_state.df.empty:
            st.write(f"Всего постов: {len(st.session_state.df)}")
            st.write(f"Уникальных авторов: {st.session_state.df['from_id'].nunique()}")
            st.write(f"Период: с {st.session_state.df['date'].min()} по {st.session_state.df['date'].max()}")
            
            group_by = st.selectbox("Группировать по:", ['hour', 'day', 'week', 'month'])
            chart = create_chart(st.session_state.df, group_by)
            st.plotly_chart(chart)
        else:
            st.info("Запустите поиск, чтобы увидеть статистику и график.")

    with tabs[1]:
        st.header("Настройки")

        # Инструкция
        with st.expander("Инструкция"):
            st.markdown("""
            1. Введите ID сообщества (например, -1 для группы ВКонтакте).
            2. Введите один или несколько API ключей VK.
            3. Введите ключевые слова или фразы для поиска, разделяя их запятыми.
            4. Выберите режим поиска: "Точная фраза" или "Частичное совпадение".
            5. Укажите начальную и конечную даты для поиска.
            6. Настройте параметры времени выполнения и паузы между запросами.
            7. Нажмите "Начать поиск" для запуска процесса.
            8. После завершения поиска вы сможете просмотреть результаты и статистику.

            ### Рекомендации по выбору паузы между запросами:
            - Начните с 5 секунд и увеличивайте, если возникают проблемы с блокировкой.
            - При использовании нескольких API ключей можно установить меньшую паузу.
            - Следите за сообщениями о блокировке и корректируйте паузу при необходимости.
            """)

        # Технические особенности и советы
        with st.expander("Технические особенности и советы"):
            st.markdown("""
            ### Технические особенности:
            - Приложение использует API VK для поиска постов и комментариев.
            - Реализовано многопоточное выполнение запросов для оптимизации скорости.
            - Используется библиотека Streamlit для создания пользовательского интерфейса.
            - Данные обрабатываются с помощью pandas и numpy.
            - Графики создаются с использованием plotly.

            ### Советы по выгрузке больших периодов:
            1. Используйте несколько API ключей для распределения нагрузки.
            2. Увеличьте паузу между запросами, чтобы избежать блокировки.
            3. Разбивайте большие периоды на несколько меньших запросов.
            4. Используйте режим "Частичное совпадение" для более быстрого поиска.
            5. Оптимизируйте ключевые слова для более точного поиска.
            6. Регулярно сохраняйте промежуточные результаты.
            7. Мониторьте процесс выполнения и корректируйте параметры при необходимости.
            """)

        # Ввод параметров
        owner_id = st.text_input("ID сообщества (например, -1 для группы ВКонтакте)")
        
        api_keys = []
        api_key = st.text_input("API ключ VK", key="api_key_0")
        api_keys.append(api_key)
        
        num_extra_keys = st.session_state.get('num_extra_keys', 0)
        
        for i in range(num_extra_keys):
            extra_key = st.text_input(f"Дополнительный API ключ VK #{i+1}", key=f"api_key_{i+1}")
            api_keys.append(extra_key)
        
        if st.button("Добавить еще один API ключ"):
            st.session_state.num_extra_keys = num_extra_keys + 1
            st.experimental_rerun()
        
        queries = st.text_area("Ключевые слова или фразы (разделите запятыми)")
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
        
        time_step = st.slider("Шаг времени (часы)", 1, 168, 24)
        pause = st.slider("Пауза между запросами (секунды)", 1, MAX_PAUSE, DEFAULT_PAUSE)

        if st.button("Начать поиск"):
            if not owner_id or not api_keys[0] or not queries:
                st.error("Пожалуйста, заполните все обязательные поля.")
            else:
                with st.spinner("Выполняется поиск..."):
                    queries_list = [q.strip() for q in queries.split(',')]
                    posts = process_posts(api_keys, owner_id, queries_list, start_datetime, end_datetime, search_mode, timedelta(hours=time_step), pause)
                    
                    if posts:
                        df = pd.DataFrame(posts)
                        df['date'] = pd.to_datetime(df['date'], unit='s')
                        df['text'] = df['text'].fillna('')
                        
                        st.session_state.df = df
                        st.success(f"Найдено {len(posts)} постов.")
                    else:
                        st.warning("Посты не найдены.")

    with tabs[2]:
        st.header("Результаты")
        if 'df' in st.session_state and not st.session_state.df.empty:
            st.write(st.session_state.df)
            
            # Добавляем возможность сортировки
            sort_column = st.selectbox("Сортировать по:", st.session_state.df.columns)
            sort_order = st.radio("Порядок сортировки:", ["По возрастанию", "По убыванию"])
            
            sorted_df = st.session_state.df.sort_values(by=sort_column, ascending=(sort_order == "По возрастанию"))
            
            # Отображаем посты и комментарии
            for _, row in sorted_df.iterrows():
                st.subheader(f"Пост от {row['date']}")
                st.write(row['text'])
                st.write(f"Лайки: {row['likes']['count']}, Репосты: {row['reposts']['count']}, Просмотры: {row.get('views', {}).get('count', 'N/A')}")
                
                if st.button(f"Показать комментарии для поста {row['id']}"):
                    comments = get_comments(api_keys[0], owner_id, row['id'])
                    if comments:
                        st.write("Комментарии:")
                        for comment in comments:
                            st.text(f"{comment['from_id']} ({format_datetime(comment['date'])}): {comment['text']}")
                    else:
                        st.info("Комментарии отсутствуют или недоступны.")
            
            # Добавляем возможность выгрузки данных
            csv = sorted_df.to_csv(index=False)
            st.download_button(
                label="Скачать данные как CSV",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )
        else:
            st.info("Запустите поиск, чтобы увидеть результаты.")

if __name__ == "__main__":
    main()