import streamlit as st
import requests
import pandas as pd
import time
import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        return json_text.get('response', {}).get('items', [])
    except Exception as e:
        st.error(f"Ошибка при получении комментариев: {e}")
        return []

def execute_query(query, start_time, end_time, access_token, include_comments, search_mode):
    url = (
        f"https://api.vk.com/method/newsfeed.search?q={query}"
        f"&count=200"
        f"&access_token={access_token}"
        f"&start_time={start_time}"
        f"&end_time={end_time}"
        f"&v=5.131"
    )

    posts = []
    comments = []

    try:
        res = requests.get(url)
        json_text = res.json()

        if 'response' in json_text and 'items' in json_text['response']:
            for item in json_text['response']['items']:
                if search_mode == 'exact':
                    if re.search(r'\b' + re.escape(query) + r'\b', item.get('text', ''), re.IGNORECASE):
                        item['matched_query'] = query
                        posts.append(item)
                else:
                    if query.lower() in item.get('text', '').lower():
                        item['matched_query'] = query
                        posts.append(item)

                if include_comments:
                    post_comments = get_comments(item['id'], item['owner_id'], access_token)
                    for comment in post_comments:
                        comment['post_id'] = item['id']
                        comment['post_owner_id'] = item['owner_id']
                    comments.extend(post_comments)

    except Exception as e:
        st.error(f"Ошибка при выполнении запроса: {e}")

    return posts, comments

def get_vk_newsfeed(queries, start_datetime, end_datetime, access_token, include_comments, progress_bar, status_text, time_sleep, search_mode, time_step):
    all_posts = []
    all_comments = []

    delta = datetime.timedelta(hours=time_step)
    current_time = start_datetime

    total_steps = int((end_datetime - start_datetime) / delta)
    step_count = 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        while current_time < end_datetime:
            step_count += 1
            futures = []
            for query in queries:
                end_time = min(current_time + delta, end_datetime)
                futures.append(executor.submit(
                    execute_query, 
                    query, 
                    get_unixtime_from_datetime(current_time),
                    get_unixtime_from_datetime(end_time),
                    access_token, 
                    include_comments, 
                    search_mode
                ))

            for future in as_completed(futures):
                posts, comments = future.result()
                all_posts.extend(posts)
                all_comments.extend(comments)

            progress = step_count / total_steps
            progress_bar.progress(progress)

            elapsed_time = time.time() - start_time
            eta = (elapsed_time / progress) - elapsed_time if progress > 0 else 0

            status_text.text(
                f"⏳ Прогресс: {progress:.2%} | ⌛ Прошло времени: {elapsed_time:.1f} сек\n"
                f"📊 Найдено постов: {len(all_posts)} | 💬 Комментариев: {len(all_comments)}\n"
                f"🕒 Текущая дата: {current_time} | ⏱️ Осталось примерно: {eta/60:.1f} мин"
            )

            current_time += delta
            time.sleep(time_sleep)

    df = pd.DataFrame(all_posts)
    comments_df = pd.DataFrame(all_comments)

    return df, comments_df

def main():
    st.set_page_config(page_title="VK Parser", page_icon="📊", layout="wide")

    st.title("📊 VK Парсер новостей и комментариев")

    with st.expander("ℹ️ Инструкция по использованию"):
        st.markdown("""
        1. 🔑 **Получите токен VK API**:
           - Перейдите на [vkhost.github.io](https://vkhost.github.io/)
           - Нажмите "Настройки", выберите "Стена" и "Доступ в любое время"
           - Нажмите "Получить" и разрешите доступ
           - Скопируйте токен из URL (между `access_token=` и `&expires_in=`)
        
        2. 📝 **Введите поисковые запросы**:
           - Каждый запрос с новой строки
           - Для точного поиска фразы, заключите её в кавычки, например: "искусственный интеллект"
           - Для поиска по отдельным словам, просто введите их, например: новости технологии
        
        3. 📅 **Выберите период поиска**:
           - Укажите начальную и конечную даты и время
           - Помните, что чем больше период, тем дольше будет выполняться парсинг
        
        4. 🔍 **Настройте параметры поиска**:
           - Выберите режим поиска (точная фраза или частичное совпадение)
           - Укажите, нужно ли включать комментарии (это может значительно увеличить время парсинга)
           - Установите шаг парсинга (в часах). Меньший шаг даёт более точные результаты, но увеличивает время работы
        
        5. 🚀 **Запустите парсинг**:
           - Нажмите кнопку "Начать парсинг"
           - Следите за прогрессом в статус-баре
        
        6. 📊 **Анализируйте результаты**:
           - Просматривайте данные в таблицах "Посты" и "Комментарии"
           - Используйте фильтры и сортировку для анализа данных
           - Загрузите результаты в CSV формате для дальнейшего анализа
        
        ⚠️ **Важно**: 
        - VK API ограничивает количество постов до 200 на один запрос
        - Большой шаг парсинга может привести к потере данных для популярных запросов
        - Маленький шаг увеличивает точность, но замедляет работу парсера
        - Экспериментируйте с настройками для оптимального баланса скорости и полноты данных
        """)

    access_token = st.text_input("🔑 Введите ваш токен доступа VK API:", type="password")

    queries = st.text_area("📝 Введите поисковые запросы (каждый с новой строки):")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("📅 Дата начала:")
    with col2:
        start_time = st.time_input("🕒 Время начала:")
    with col3:
        end_date = st.date_input("📅 Дата окончания:")
    with col4:
        end_time = st.time_input("🕒 Время окончания:")
    
    start_datetime = datetime.datetime.combine(start_date, start_time)
    end_datetime = datetime.datetime.combine(end_date, end_time)
    
    include_comments = st.checkbox("💬 Включить комментарии", value=True)
    time_sleep = st.slider("⏱️ Пауза между запросами (секунды)", min_value=0.1, max_value=2.0, value=0.5, step=0.1)

    search_mode = st.radio("🔍 Режим поиска:", ["Точная фраза", "Частичное совпадение"])
    
    time_step = st.slider("📊 Шаг парсинга (часы)", min_value=1, max_value=24, value=1, step=1)
    
    if 'full_df' not in st.session_state:
        st.session_state.full_df = None
    if 'comments_df' not in st.session_state:
        st.session_state.comments_df = None

    start_parsing = st.button("🚀 Начать парсинг")

    if start_parsing:
        if not access_token or not queries or not start_date or not end_date:
            st.error("Пожалуйста, заполните все поля.")
            return

        if (end_datetime - start_datetime).total_seconds() < 3600:
            st.error("Минимальный период парсинга должен быть не менее 1 часа.")
            return

        queries_list = [q.strip() for q in queries.split('\n') if q.strip()]

        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Парсинг начался...")
        df, comments_df = get_vk_newsfeed(queries_list, start_datetime, end_datetime, 
                                          access_token, include_comments, progress_bar, status_text, time_sleep,
                                          'exact' if search_mode == "Точная фраза" else 'partial', time_step)
        status_text.text("Парсинг завершен!")

        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], unit='s')
            
            columns_order = ['matched_query', 'text', 'date', 'id', 'owner_id', 'from_id', 'likes', 'reposts', 'views', 'comments']
            df = df.reindex(columns=columns_order + [col for col in df.columns if col not in columns_order])

            st.session_state.full_df = df
            st.session_state.comments_df = comments_df
        else:
            st.warning("Данные не найдены для указанных параметров.")

    if st.session_state.full_df is not None:
        st.subheader("📊 Результаты парсинга")
        
        tab1, tab2 = st.tabs(["📝 Посты", "💬 Комментарии"])
        
        with tab1:
            st.dataframe(st.session_state.full_df)
            csv = st.session_state.full_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать посты (CSV)",
                data=csv,
                file_name="vk_posts.csv",
                mime="text/csv",
            )
        
        with tab2:
            if include_comments and not st.session_state.comments_df.empty:
                st.dataframe(st.session_state.comments_df)
                comments_csv = st.session_state.comments_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Скачать комментарии (CSV)",
                    data=comments_csv,
                    file_name="vk_comments.csv",
                    mime="text/csv",
                )
            else:
                st.info("Комментарии не были включены в парсинг или не найдены.")

if __name__ == "__main__":
    main()