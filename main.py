import streamlit as st
import requests
import pandas as pd
import time
import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Функция для преобразования datetime в UNIX-время
def get_unixtime_from_datetime(dt):
    """
    Преобразует объект datetime в UNIX-время.
    
    :param dt: объект datetime
    :return: UNIX-время (целое число)
    """
    return int(time.mktime(dt.timetuple()))

# Функция для получения комментариев к посту
def get_comments(post_id, owner_id, access_token):
    """
    Получает комментарии к посту по его ID и ID владельца.
    
    :param post_id: ID поста
    :param owner_id: ID владельца поста
    :param access_token: токен доступа VK API
    :return: список комментариев
    """
    url = (
        f"https://api.vk.com/method/wall.getComments?"
        f"owner_id={owner_id}&post_id={post_id}&access_token={access_token}&v=5.131"
    )
    try:
        res = requests.get(url)
        json_text = res.json()
        if 'response' in json_text and 'items' in json_text['response']:
            return json_text['response']['items']
        else:
            return []
    except Exception as e:
        st.error(f"Ошибка при получении комментариев: {e}")
        return []

# Функция для выполнения одного запроса к VK API
def execute_query(query, current_time, delta, access_token, include_comments, search_mode):
    """
    Выполняет один запрос к VK API для поиска постов.
    
    :param query: поисковый запрос
    :param current_time: текущее время для запроса
    :param delta: временной интервал для запроса
    :param access_token: токен доступа VK API
    :param include_comments: флаг для включения комментариев
    :param search_mode: режим поиска ('exact' или 'partial')
    :return: кортеж из списка постов и списка комментариев
    """
    url = (
        f"https://api.vk.com/method/newsfeed.search?q={query}"
        f"&count=200"
        f"&access_token={access_token}"
        f"&start_time={get_unixtime_from_datetime(current_time)}"
        f"&end_time={get_unixtime_from_datetime(current_time + delta)}"
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
                    if re.search(r'\b' + re.escape(query) + r'\b', item['text'], re.IGNORECASE):
                        item['matched_query'] = query
                        posts.append(item)
                else:
                    if query.lower() in item['text'].lower():
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

# Основная функция для получения новостной ленты VK
def get_vk_newsfeed(queries, start_datetime, end_datetime, access_token, include_comments, progress_bar, time_sleep, search_mode):
    """
    Получает новостную ленту VK на основе заданных параметров.
    
    :param queries: список поисковых запросов
    :param start_datetime: начальная дата и время поиска
    :param end_datetime: конечная дата и время поиска
    :param access_token: токен доступа VK API
    :param include_comments: флаг для включения комментариев
    :param progress_bar: объект для отображения прогресса
    :param time_sleep: время задержки между запросами
    :param search_mode: режим поиска ('exact' или 'partial')
    :return: кортеж из DataFrame с постами и DataFrame с комментариями
    """
    all_posts = []
    all_comments = []

    delta = datetime.timedelta(days=1)
    current_time = start_datetime

    total_seconds = (end_datetime - start_datetime).total_seconds()
    start_time = time.time()

    # Используем ThreadPoolExecutor для параллельного выполнения запросов
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_query = {}
        while current_time <= end_datetime:
            for query in queries:
                future = executor.submit(execute_query, query, current_time, delta, access_token, include_comments, search_mode)
                future_to_query[future] = query

            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    posts, comments = future.result()
                    all_posts.extend(posts)
                    all_comments.extend(comments)
                except Exception as e:
                    st.error(f"Ошибка при обработке запроса '{query}': {e}")

            elapsed_time = time.time() - start_time
            progress = min(elapsed_time / total_seconds, 1.0)
            progress_bar.progress(progress)

            current_time += delta
            time.sleep(time_sleep)

    df = pd.DataFrame(all_posts)
    comments_df = pd.DataFrame(all_comments)

    return df, comments_df

# Функция для отображения поста с комментариями
def display_post_with_comments(post, comments):
    """
    Отображает пост с его комментариями.
    
    :param post: словарь с данными поста
    :param comments: список комментариев к посту
    """
    st.write(f"**📌 Post ID:** {post['id']}")
    st.write(f"**🕒 Date:** {post['date']}")
    st.write(f"**📝 Text:** {post['text']}")
    st.write(f"**🔍 Matched Query:** {post['matched_query']}")
    st.write(f"👍 {post.get('likes', {}).get('count', 'N/A')} | 🔁 {post.get('reposts', {}).get('count', 'N/A')} | 👀 {post.get('views', {}).get('count', 'N/A')}")
    st.write("**💬 Comments:**")
    for comment in comments:
        st.text(f"👤 {comment['from_id']} ({comment['date']}): {comment['text']}")
    st.write("---")

# Основная функция приложения
def main():
    # Выбор языка
    lang = st.sidebar.selectbox("Language / Язык", ["English", "Русский"])

    # Словарь с текстами на разных языках
    texts = {
        "English": {
            "title": "📊 VK News and Comments Parser",
            "description": "This application allows you to search for posts and comments on VK (VKontakte) using keywords or phrases. You can specify the time period, include comments, and view the results in various formats.",
            "token_instruction": "🔑 How to get VK API access token",
            "token_input": "Enter your VK API access token:",
            "queries_instruction": "Enter your search queries. Each query should be on a new line.",
            "queries_input": "Enter keywords or expressions (one per line):",
            "start_date": "Start date:",
            "start_time": "Start time:",
            "end_date": "End date:",
            "end_time": "End time:",
            "include_comments": "Include comments",
            "time_sleep": "Time sleep between requests (seconds)",
            "start_parsing": "🚀 Start Parsing",
            "select_columns": "Select columns to display and save",
            "posts": "📝 Posts",
            "comments": "💬 Comments",
            "display_option": "Choose display option",
            "table_view": "Table view",
            "post_view": "Post view",
            "sort_posts": "Sort posts by",
            "most_commented": "Most commented",
            "newest": "Newest",
            "oldest": "Oldest",
            "top_posts": "Number of top posts to display",
            "search_mode": "Search mode",
            "exact_search": "Exact phrase",
            "partial_search": "Partial match",
            "search_mode_instruction": "Choose the search mode:",
            "token_instructions": """
            To generate an `access token`:
            1. Go to https://vkhost.github.io/
            2. Click on `Settings »`
            3. Select `Wall` and `Access at any time`
            4. Click on the `Get` button
            5. Confirm access to your account by clicking `Allow`
            6. In the resulting URL, find the part between `access_token=` and `&expires_in=`
            7. Copy this token and paste it in the field below
            """,
            "search_mode_example": """
            Example:
            - Exact phrase: "data science" will find posts containing exactly "data science"
            - Partial match: "data science" will find posts containing "data" or "science" separately
            """
        },
        "Русский": {
            "title": "📊 Парсер новостей и комментариев ВКонтакте",
            "description": "Это приложение позволяет искать посты и комментарии во ВКонтакте, используя ключевые слова или фразы. Вы можете указать временной период, включить комментарии и просматривать результаты в различных форматах.",
            "token_instruction": "🔑 Как получить токен доступа VK API",
            "token_input": "Введите ваш токен доступа VK API:",
            "queries_instruction": "Введите ваши поисковые запросы. Каждый запрос должен быть на новой строке.",
            "queries_input": "Введите ключевые слова или выражения (по одному на строку):",
            "start_date": "Дата начала:",
            "start_time": "Время начала:",
            "end_date": "Дата окончания:",
            "end_time": "Время окончания:",
            "include_comments": "Включить комментарии",
            "time_sleep": "Пауза между запросами (секунды)",
            "start_parsing": "🚀 Начать парсинг",
            "select_columns": "Выберите столбцы для отображения и сохранения",
            "posts": "📝 Посты",
            "comments": "💬 Комментарии",
            "display_option": "Выберите вариант отображения",
            "table_view": "Табличный вид",
            "post_view": "Вид постов",
            "sort_posts": "Сортировать посты по",
            "most_commented": "Самые комментируемые",
            "newest": "Новейшие",
            "oldest": "Старейшие",
            "top_posts": "Количество отображаемых топ-постов",
            "search_mode": "Режим поиска",
            "exact_search": "Точная фраза",
            "partial_search": "Частичное совпадение",
            "search_mode_instruction": "Выберите режим поиска:",
            "token_instructions": """
            Для генерации `access token` необходимо:
            1. Перейти на сайт https://vkhost.github.io/
            2. Нажать на `Настройки »`
            3. Выбрать пункты `Стена` и `Доступ в любое время`
            4. Нажать на кнопку `Получить`
            5. Подтвердить доступ к вашему аккаунту, нажав `Разрешить`
            6. В появившемся URL найдите часть между `access_token=` и `&expires_in=`
            7. Скопируйте этот токен и вставьте его в поле ниже
            """,
            "search_mode_example": """
            Пример:
            - Точная фраза: "наука о данных" найдет посты, содержащие точно "наука о данных"
            - Частичное совпадение: "наука о данных" найдет посты, содержащие "наука" или "данных" по отдельности
            """
        }
    }

    t = texts[lang]

    st.title(t["title"])
    st.write(t["description"])

    with st.expander(t["token_instruction"]):
        st.markdown(t["token_instructions"])

    access_token = st.text_input(t["token_input"], type="password")

    st.write(t["queries_instruction"])
    queries = st.text_area(t["queries_input"])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input(t["start_date"])
    with col2:
        start_time = st.time_input(t["start_time"])
    with col3:
        end_date = st.date_input(t["end_date"])
    with col4:
        end_time = st.time_input(t["end_time"])
    
    start_datetime = datetime.datetime.combine(start_date, start_time)
    end_datetime = datetime.datetime.combine(end_date, end_time)
    
    include_comments = st.checkbox(t["include_comments"], value=True)
    time_sleep = st.slider(t["time_sleep"], min_value=0.1, max_value=6.0, value=0.5, step=0.1)

    st.write(t["search_mode_instruction"])
    search_mode = st.radio(t["search_mode"], [t["exact_search"], t["partial_search"]])
    st.info(t["search_mode_example"])

    if 'full_df' not in st.session_state:
        st.session_state.full_df = None
    if 'comments_df' not in st.session_state:
        st.session_state.comments_df = None

    start_parsing = st.button(t["start_parsing"])

    if start_parsing:
        if not access_token or not queries or not start_date or not end_date:
            st.error("Please fill in all fields.")
            return

        if (end_datetime - start_datetime).total_seconds() < 86400:  # 86400 seconds in a day
            st.error("The minimum parsing period should be at least 1 day.")
            return

        queries_list = [q.strip() for q in queries.split('\n') if q.strip()]

        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Parsing in progress...")
        df, comments_df = get_vk_newsfeed(queries_list, start_datetime, end_datetime, 
                                          access_token, include_comments, progress_bar, time_sleep,
                                          'exact' if search_mode == t["exact_search"] else 'partial')
        status_text.text("Parsing completed!")

        if not df.empty:
            # Convert Unix timestamp to readable date
            df['date'] = pd.to_datetime(df['date'], unit='s')

            # Store the full dataset in session state
            st.session_state.full_df = df
            st.session_state.comments_df = comments_df
        else:
            st.warning("No data found for the given parameters.")

    if st.session_state.full_df is not None:
        # Allow user to select columns after data is loaded
        all_columns = st.session_state.full_df.columns.tolist()
        selected_columns = st.multiselect(t["select_columns"], all_columns, default=all_columns, key='selected_columns')

        st.subheader(t["posts"])
        st.write(st.session_state.full_df[selected_columns])

        csv = st.session_state.full_df[selected_columns].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Posts CSV",
            data=csv,
            file_name="vk_posts.csv",
            mime="text/csv",
        )

        if include_comments and not st.session_state.comments_df.empty:
            st.subheader(t["comments"])
            display_option = st.radio(t["display_option"], [t["table_view"], t["post_view"]])
            
            if display_option == t["table_view"]:
                st.write(st.session_state.comments_df)

                comments_csv = st.session_state.comments_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Comments CSV",
                    data=comments_csv,
                    file_name="vk_comments.csv",
                    mime="text/csv",
                )
            else:  # Post view
                # Prepare data for post view
                posts_with_comments = st.session_state.full_df.copy()
                posts_with_comments['comments'] = posts_with_comments['id'].apply(
                    lambda x: st.session_state.comments_df[st.session_state.comments_df['post_id'] == x].to_dict('records')
                )
                posts_with_comments['comment_count'] = posts_with_comments['comments'].apply(len)

                # Sorting options
                sort_option = st.selectbox(t["sort_posts"], [t["most_commented"], t["newest"], t["oldest"]])
                if sort_option == t["most_commented"]:
                    posts_with_comments = posts_with_comments.sort_values('comment_count', ascending=False)
                elif sort_option == t["newest"]:
                    posts_with_comments = posts_with_comments.sort_values('date', ascending=False)
                else:  # Oldest
                    posts_with_comments = posts_with_comments.sort_values('date')

                # Number of top posts to display
                top_n = st.slider(t["top_posts"], min_value=1, max_value=len(posts_with_comments), value=5)

                # Display posts with comments
                for _, post in posts_with_comments.head(top_n).iterrows():
                    display_post_with_comments(post, post['comments'])

if __name__ == "__main__":
    main()