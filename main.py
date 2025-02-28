import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import pytz
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

def check_api_key(api_key):
    url = f"https://api.vk.com/method/users.get?access_token={api_key}&v=5.131"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'error' in data and data['error']['error_code'] == 5:
            return False, "API ключ недействителен или не имеет необходимых прав доступа."
        elif 'error' in data and data['error']['error_code'] == 29:
            return False, "API ключ временно заблокирован из-за превышения лимита запросов."
        return True, "API ключ действителен."
    return False, "Не удалось проверить API ключ. Пожалуйста, попробуйте позже."

def execute_query(api_key, owner_id, query, start_time, end_time, search_mode, time_step):
    all_posts = []
    current_time = start_time
    
    while current_time < end_time:
        next_time = min(current_time + timedelta(seconds=time_step), end_time)
        
        url = f"https://api.vk.com/method/wall.search?owner_id={owner_id}&query={query}&count=100&access_token={api_key}&v=5.131"
        url += f"&start_time={int(current_time.timestamp())}&end_time={int(next_time.timestamp())}"
        
        response = requests.get(url)
        data = response.json()
        
        if 'error' in data:
            if data['error']['error_code'] == 29:
                return None, "API ключ временно заблокирован из-за превышения лимита запросов."
            else:
                return None, f"Ошибка API: {data['error']['error_msg']}"
        
        if 'response' in data and 'items' in data['response']:
            posts = data['response']['items']
            for post in posts:
                if 'text' in post:
                    if search_mode == 'exact' and query.lower() not in post['text'].lower():
                        continue
                    all_posts.append(post)
        
        current_time = next_time
    
    return all_posts, None

def format_number(num):
    if num >= 1000000:
        return f"{num/1000000:.1f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    else:
        return str(num)

def main():
    st.set_page_config(page_title="VK News Scraper", page_icon=":newspaper:", layout="wide")
    st.title("VK News Scraper")

    # Инструкция
    with st.expander("Инструкция"):
        st.markdown("""
        1. Введите свой API ключ VK. Если у вас его нет, получите его на [странице разработчиков VK](https://vk.com/dev).
        2. Введите ID сообщества (например, -1 для новостей ВКонтакте) или пользователя.
        3. Выберите режим поиска: точное совпадение или частичное.
        4. Введите ключевое слово или фразу для поиска.
        5. Выберите диапазон дат для поиска.
        6. Настройте паузу между запросами. Рекомендуется устанавливать не менее 0.5 секунд для избежания блокировки.
        7. Нажмите "Начать поиск" для запуска процесса.
        8. Результаты будут отображены в виде таблицы, которую можно скачать в формате CSV.

        **Новый режим с несколькими API ключами:**
        - Вы можете добавить несколько API ключей для распределения нагрузки и ускорения процесса.
        - Каждый ключ будет использоваться поочередно, что снижает риск блокировки.
        - Рекомендуется использовать этот режим при больших объемах данных или частых запросах.

        **Рекомендации по настройке паузы:**
        - Для небольших запросов (до 1000 постов) достаточно 0.5-1 секунды.
        - Для средних запросов (1000-10000 постов) рекомендуется 1-3 секунды.
        - Для больших запросов (более 10000 постов) установите паузу 3-5 секунд.
        - При использовании нескольких API ключей можно уменьшить паузу, но не менее 0.3 секунды на ключ.
        """)

    # Ввод API ключей
    api_keys_input = st.text_area("Введите API ключи (по одному на строку):", height=100)
    api_keys = [key.strip() for key in api_keys_input.split('\n') if key.strip()]

    if not api_keys:
        st.warning("Пожалуйста, введите хотя бы один API ключ.")
        return

    # Проверка API ключей
    valid_keys = []
    for key in api_keys:
        is_valid, message = check_api_key(key)
        if is_valid:
            valid_keys.append(key)
        else:
            st.warning(f"Ключ {key[:5]}... недействителен: {message}")

    if not valid_keys:
        st.error("Нет действительных API ключей. Пожалуйста, проверьте введенные ключи.")
        return

    st.success(f"Найдено {len(valid_keys)} действительных API ключей.")

    owner_id = st.text_input("ID сообщества или пользователя (например, -1 для новостей ВКонтакте):")
    search_mode = st.radio("Режим поиска:", ('exact', 'partial'))
    query = st.text_input("Ключевое слово или фраза для поиска:")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Начальная дата:")
        start_time = st.time_input("Начальное время:", value=datetime.min.time())
    with col2:
        end_date = st.date_input("Конечная дата:")
        end_time = st.time_input("Конечное время:", value=datetime.max.time())

    start_datetime = datetime.combine(start_date, start_time).replace(tzinfo=pytz.UTC)
    end_datetime = datetime.combine(end_date, end_time).replace(tzinfo=pytz.UTC)

    time_step = st.slider("Шаг времени (секунды):", min_value=60, max_value=86400, value=3600, step=60)
    pause = st.slider("Пауза между запросами (секунды):", min_value=0.3, max_value=10.0, value=5.0, step=0.1)

    if st.button("Начать поиск"):
        if not owner_id or not query:
            st.warning("Пожалуйста, заполните все поля.")
            return

        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()

        all_posts = []
        total_steps = (end_datetime - start_datetime).total_seconds() / time_step
        completed_steps = 0

        def process_chunk(args):
            api_key, chunk_start, chunk_end = args
            chunk_posts, error = execute_query(api_key, owner_id, query, chunk_start, chunk_end, search_mode, time_step)
            if error:
                return None, error
            return chunk_posts, None

        with ThreadPoolExecutor(max_workers=len(valid_keys)) as executor:
            futures = []
            current_time = start_datetime
            key_index = 0

            while current_time < end_datetime:
                next_time = min(current_time + timedelta(seconds=time_step), end_datetime)
                api_key = valid_keys[key_index]
                futures.append(executor.submit(process_chunk, (api_key, current_time, next_time)))
                
                key_index = (key_index + 1) % len(valid_keys)
                current_time = next_time
                time.sleep(pause)

            for future in as_completed(futures):
                chunk_posts, error = future.result()
                if error:
                    st.error(f"Ошибка при выполнении запроса: {error}")
                    return
                if chunk_posts:
                    all_posts.extend(chunk_posts)
                
                completed_steps += 1
                progress = completed_steps / total_steps
                progress_bar.progress(progress)
                
                elapsed_time = time.time() - start_time
                estimated_total_time = elapsed_time / progress if progress > 0 else 0
                remaining_time = estimated_total_time - elapsed_time
                
                status_text.text(f"Обработано {completed_steps:.0f} из {total_steps:.0f} шагов. "
                                 f"Прошло времени: {elapsed_time:.0f} сек. "
                                 f"Осталось примерно: {remaining_time:.0f} сек.")

        if all_posts:
            df = pd.DataFrame(all_posts)
            df['date'] = pd.to_datetime(df['date'], unit='s')
            df['likes'] = df['likes'].apply(lambda x: x['count'] if isinstance(x, dict) and 'count' in x else 0)
            df['reposts'] = df['reposts'].apply(lambda x: x['count'] if isinstance(x, dict) and 'count' in x else 0)
            df['views'] = df['views'].apply(lambda x: x['count'] if isinstance(x, dict) and 'count' in x else 0)
            
            df['likes'] = df['likes'].apply(format_number)
            df['reposts'] = df['reposts'].apply(format_number)
            df['views'] = df['views'].apply(format_number)
            
            df = df[['id', 'date', 'text', 'likes', 'reposts', 'views']]
            df.columns = ['ID', 'Дата', 'Текст', 'Лайки', 'Репосты', 'Просмотры']
            
            st.write(f"Найдено {len(df)} постов")
            st.dataframe(df)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Скачать данные как CSV",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )
        else:
            st.warning("Посты не найдены.")

if __name__ == "__main__":
    main()