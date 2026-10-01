import sys
import re
import time
import sqlite3
import json
import os
import urllib.parse
import requests
from datetime import datetime, timedelta
import threading
import traceback
from typing import Optional, List, Tuple
from http.server import HTTPServer, BaseHTTPRequestHandler

print("=" * 60)
print("🚀 БОТ КОММЕНТАРИЕВ ЗАПУСКАЕТСЯ (Callback API + Comments Check)...")
print("=" * 60)
sys.stdout.flush()

try:
    import vk_api
    from vk_api.exceptions import ApiError
    print("✅ Библиотека vk-api загружена")
    sys.stdout.flush()
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.stdout.flush()
    raise

# ====================== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ======================
GROUP_TOKEN = os.getenv('GROUP_TOKEN', '')
USER_TOKEN = os.getenv('USER_TOKEN', '')
GROUP_ID = int(os.getenv('GROUP_ID', '241663340'))
CONFIRMATION_CODE = os.getenv('CONFIRMATION_CODE', '5a3fed15')
PORT = int(os.getenv('PORT', '3000'))
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '447457340')
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip()]
DELETE_AFTER = 300
MIN_COMMENT_LENGTH = 10
# =============================================================================

MAX_QUEUE_SIZE = 5
VIP_DURATION_HOURS = 24
RATE_LIMIT_DELAY = 0.5
DB_FILE = "comments_bot.db"

queue = []
queue_lock = threading.Lock()
vip_links = []
vip_links_lock = threading.Lock()
vk_group = None
vk_user = None

user_activity = {}
activity_lock = threading.Lock()

user_name_cache = {}
user_name_cache_lock = threading.Lock()
USER_NAME_CACHE_TTL = 3600

pending_deletions = []
deletions_lock = threading.Lock()

VK_API_VERSION = "5.131"


def make_clickable_link(vk_link: str) -> str:
    if not vk_link:
        return vk_link
    if vk_link.startswith('http'):
        return vk_link
    return f"https://vk.com/{vk_link}"


def is_owner(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_user_name(user_id: int) -> str:
    global vk_user
    if vk_user is None:
        return ""
    
    now = time.time()
    
    with user_name_cache_lock:
        if user_id in user_name_cache:
            name, ts = user_name_cache[user_id]
            if now - ts < USER_NAME_CACHE_TTL:
                return name
    
    try:
        rate_limit()
        user_info = vk_user.users.get(user_ids=[user_id])[0]
        name = f"{user_info['first_name']} {user_info['last_name']}"
        
        with user_name_cache_lock:
            user_name_cache[user_id] = (name, now)
        
        return name
    except Exception as e:
        print(f"⚠️ Не удалось получить имя пользователя {user_id}: {e}", flush=True)
        return ""


def get_mention(user_id: int) -> str:
    name = get_user_name(user_id)
    if name:
        return f"[id{user_id}|{name}]"
    return f"[id{user_id}|пользователь]"


def init_database():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                is_owner_post INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vip_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link TEXT NOT NULL UNIQUE,
                added_by INTEGER NOT NULL,
                expires_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_activity (
                user_id INTEGER PRIMARY KEY,
                last_post_time TEXT,
                post_count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id INTEGER NOT NULL,
                conv_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        ''')
        conn.commit()
        conn.close()
        print("✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
    sys.stdout.flush()


def load_data():
    global queue, vip_links, user_activity, pending_deletions
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT link, user_id, timestamp, is_owner_post FROM queue ORDER BY id DESC LIMIT ?', (MAX_QUEUE_SIZE,))
        rows = cursor.fetchall()
        queue = []
        for row in reversed(rows):
            queue.append({
                'link': row[0],
                'user_id': row[1],
                'timestamp': datetime.fromisoformat(row[2]),
                'is_owner_post': row[3] if len(row) > 3 else 0
            })
        cursor.execute('SELECT link, added_by, expires_at FROM vip_links')
        vip_rows = cursor.fetchall()
        vip_links = []
        now = datetime.now()
        for row in vip_rows:
            expires_at = datetime.fromisoformat(row[2])
            if expires_at > now:
                vip_links.append({
                    'link': row[0],
                    'added_by': row[1],
                    'expires_at': expires_at
                })

        cursor.execute('SELECT user_id, last_post_time, post_count FROM user_activity')
        for row in cursor.fetchall():
            user_activity[row[0]] = {
                'last_post_time': datetime.fromisoformat(row[1]) if row[1] else None,
                'post_count': row[2]
            }

        cursor.execute('SELECT peer_id, conv_message_id, created_at FROM bot_messages')
        for row in cursor.fetchall():
            pending_deletions.append({
                'peer_id': row[0],
                'conv_message_id': row[1],
                'created_at': datetime.fromisoformat(row[2])
            })

        conn.close()
        print(f"📂 Загружено: {len(queue)} ссылок, {len(vip_links)} VIP, {len(pending_deletions)} на удаление")
    except Exception as e:
        print(f"⚠️ Ошибка загрузки: {e}")
    sys.stdout.flush()


def save_bot_message(peer_id: int, conv_message_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO bot_messages (peer_id, conv_message_id, created_at) VALUES (?, ?, ?)',
                      (peer_id, conv_message_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения: {e}")


def remove_bot_message(conv_message_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM bot_messages WHERE conv_message_id = ?', (conv_message_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка удаления из БД: {e}")


def save_user_activity(user_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_activity (user_id, last_post_time, post_count)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_post_time = excluded.last_post_time,
                post_count = excluded.post_count
        ''', (user_id, datetime.now().isoformat(), user_activity.get(user_id, {}).get('post_count', 0) + 1))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения активности: {e}")


def save_queue():
    global queue
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM queue')
        for item in queue:
            cursor.execute('INSERT INTO queue (link, user_id, timestamp, is_owner_post) VALUES (?, ?, ?, ?)',
                          (item['link'], item['user_id'], item['timestamp'].isoformat(), item.get('is_owner_post', 0)))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения очереди: {e}")


def save_vip_links():
    global vip_links
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM vip_links')
        for item in vip_links:
            cursor.execute('INSERT INTO vip_links (link, added_by, expires_at) VALUES (?, ?, ?)',
                          (item['link'], item['added_by'], item['expires_at'].isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения VIP: {e}")


def reload_vip_links():
    global vip_links
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT link, added_by, expires_at FROM vip_links')
        vip_rows = cursor.fetchall()
        vip_links = []
        now = datetime.now()
        for row in vip_rows:
            expires_at = datetime.fromisoformat(row[2])
            if expires_at > now:
                vip_links.append({
                    'link': row[0],
                    'added_by': row[1],
                    'expires_at': expires_at
                })
        conn.close()
        print(f"🔄 VIP-ссылки перезагружены: {len(vip_links)}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка перезагрузки VIP: {e}", flush=True)


def reload_queue():
    global queue
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT link, user_id, timestamp, is_owner_post FROM queue ORDER BY id DESC LIMIT ?', (MAX_QUEUE_SIZE,))
        rows = cursor.fetchall()
        queue = []
        for row in reversed(rows):
            queue.append({
                'link': row[0],
                'user_id': row[1],
                'timestamp': datetime.fromisoformat(row[2]),
                'is_owner_post': row[3] if len(row) > 3 else 0
            })
        conn.close()
        print(f"🔄 Очередь перезагружена: {len(queue)}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка перезагрузки очереди: {e}", flush=True)


def cleanup_expired_vip():
    global vip_links
    with vip_links_lock:
        now = datetime.now()
        vip_links = [v for v in vip_links if v['expires_at'] > now]
        save_vip_links()


def cleanup_old_queue():
    global queue
    with queue_lock:
        if len(queue) > MAX_QUEUE_SIZE:
            queue = queue[-MAX_QUEUE_SIZE:]
            save_queue()


def rate_limit():
    time.sleep(RATE_LIMIT_DELAY)


# ====================== ПАРСИНГ ССЫЛОК ======================

def parse_content_link(text: str) -> Optional[tuple]:
    if not text:
        return None
    
    text = text.strip()
    
    match = re.search(r'(wall-?\d+_\d+)', text)
    if match:
        parts = match.group(1).split('_')
        owner_id = int(parts[0][4:])
        item_id = int(parts[1])
        return 'post', owner_id, item_id
    
    match = re.search(r'(photo-?\d+_\d+)', text)
    if match:
        parts = match.group(1).split('_')
        owner_id = int(parts[0][5:])
        item_id = int(parts[1])
        return 'photo', owner_id, item_id
    
    match = re.search(r'(video-?\d+_\d+)', text)
    if match:
        parts = match.group(1).split('_')
        owner_id = int(parts[0][5:])
        item_id = int(parts[1])
        return 'video', owner_id, item_id
    
    match = re.search(r'(clip-?\d+_\d+)', text)
    if match:
        parts = match.group(1).split('_')
        owner_id = int(parts[0][4:])
        item_id = int(parts[1])
        return 'video', owner_id, item_id
    
    return None


def extract_vk_link(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r'(wall-?\d+_\d+)',
        r'(photo-?\d+_\d+)',
        r'(video-?\d+_\d+)',
        r'(clip-?\d+_\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


# ====================== ПРОВЕРКА ОТКРЫТОСТИ КОММЕНТАРИЕВ ======================

def can_comment_on_content(content_type: str, owner_id: int, item_id: int) -> tuple:
    """
    Проверяет, открыты ли комментарии у контента.
    Использует пользовательский токен (vk_user).
    Возвращает: (можно_комментировать, причина)
    """
    global vk_user
    if vk_user is None:
        return False, "Бот не подключен"
    
    try:
        rate_limit()
        
        if content_type == 'post':
            response = vk_user.wall.getById(
                posts=f"{owner_id}_{item_id}",
                extended=0
            )
            items = response.get('items', []) if isinstance(response, dict) else response
            if not items:
                return False, "Пост не найден"
            
            post = items[0]
            comments_info = post.get('comments', {})
            
            # Если поля can_post нет — считаем открытыми
            if 'can_post' not in comments_info:
                return True, ""
            
            can_post = comments_info.get('can_post', 0)
            
            if can_post == 0:
                return False, "Комментарии к посту закрыты"
            return True, ""
            
        elif content_type == 'photo':
            response = vk_user.photos.getById(
                photos=f"{owner_id}_{item_id}",
                extended=1
            )
            
            items = response.get('items', []) if isinstance(response, dict) else response
            if not items:
                return False, "Фото не найдено"
            
            photo = items[0]
            
            if 'can_comment' not in photo:
                return True, ""
            
            can_comment = photo.get('can_comment', 0)
            
            if can_comment == 0:
                return False, "Комментарии к фото закрыты"
            return True, ""
            
        elif content_type == 'video':
            response = vk_user.video.get(
                videos=f"{owner_id}_{item_id}",
                extended=1
            )
            
            items = response.get('items', []) if isinstance(response, dict) else response
            if not items:
                return False, "Видео не найдено"
            
            video = items[0]
            
            if 'can_comment' not in video:
                return True, ""
            
            can_comment = video.get('can_comment', 0)
            
            if can_comment == 0:
                return False, "Комментарии к видео закрыты"
            return True, ""
        
        return False, "Неизвестный тип контента"
        
    except ApiError as e:
        error_msg = str(e)
        print(f"   ⚠️ can_comment_on_content ошибка: {error_msg}", flush=True)
        # Не смогли проверить — пропускаем
        return True, ""
    except Exception as e:
        print(f"   ⚠️ can_comment_on_content ошибка: {e}", flush=True)
        return True, ""


# ====================== ПРОВЕРКА КОММЕНТАРИЕВ ======================

def is_quality_comment(text: str) -> tuple:
    if not text:
        return 'bad', "Комментарий пустой или содержит только стикер/смайлик"
    
    text = text.strip()
    
    if len(text) < MIN_COMMENT_LENGTH:
        return 'bad', f"Комментарий слишком короткий ({len(text)} символов, минимум {MIN_COMMENT_LENGTH})"
    
    if not re.search(r'[а-яА-Яa-zA-Z]', text):
        return 'bad', "Комментарий должен содержать текст, а не только смайлики"
    
    if not re.search(r'[а-яА-Яa-zA-Z]{3,}', text):
        return 'bad', "Комментарий должен содержать осмысленный текст"
    
    spam_words = ['спасибо', 'привет', 'лайк', 'подпишись', 'взаимно', 'класс', 'супер', 'ок', 'норм']
    words = text.lower().split()
    if len(words) <= 2 and any(w in spam_words for w in words):
        return 'bad', "Комментарий слишком однообразный, напишите развёрнутый отзыв"
    
    return 'ok', ""


def find_comment_in_post(owner_id: int, post_id: int, user_id: int) -> Optional[dict]:
    """Ищет комментарий под постом через пользовательский токен"""
    global vk_user
    if vk_user is None:
        return None
    
    offset = 0
    count = 100
    
    while True:
        try:
            rate_limit()
            response = vk_user.wall.getComments(
                owner_id=owner_id,
                post_id=post_id,
                count=count,
                offset=offset,
                sort='desc',
                preview_length=0
            )
            
            comments = response.get('items', [])
            total = response.get('count', 0)
            
            if not comments:
                break
            
            for comment in comments:
                if comment.get('from_id') == user_id:
                    return comment
            
            offset += count
            if offset >= total:
                break
                
        except Exception as e:
            print(f"⚠️ Ошибка получения комментариев к посту {owner_id}_{post_id}: {e}", flush=True)
            return None
    
    return None


def find_comment_in_photo(owner_id: int, photo_id: int, user_id: int) -> Optional[dict]:
    """Ищет комментарий под фото через пользовательский токен"""
    global vk_user
    if vk_user is None:
        return None
    
    offset = 0
    count = 100
    
    while True:
        try:
            rate_limit()
            response = vk_user.photos.getComments(
                owner_id=owner_id,
                photo_id=photo_id,
                count=count,
                offset=offset,
                sort='desc'
            )
            
            comments = response.get('items', [])
            total = response.get('count', 0)
            
            if not comments:
                break
            
            for comment in comments:
                if comment.get('from_id') == user_id:
                    return comment
            
            offset += count
            if offset >= total:
                break
                
        except Exception as e:
            print(f"⚠️ Ошибка получения комментариев к фото {owner_id}_{photo_id}: {e}", flush=True)
            return None
    
    return None


def find_comment_in_video(owner_id: int, video_id: int, user_id: int) -> Optional[dict]:
    """Ищет комментарий под видео через пользовательский токен"""
    global vk_user
    if vk_user is None:
        return None
    
    offset = 0
    count = 100
    
    while True:
        try:
            rate_limit()
            response = vk_user.video.getComments(
                owner_id=owner_id,
                video_id=video_id,
                count=count,
                offset=offset,
                sort='desc'
            )
            
            comments = response.get('items', [])
            total = response.get('count', 0)
            
            if not comments:
                break
            
            for comment in comments:
                if comment.get('from_id') == user_id:
                    return comment
            
            offset += count
            if offset >= total:
                break
                
        except Exception as e:
            print(f"⚠️ Ошибка получения комментариев к видео {owner_id}_{video_id}: {e}", flush=True)
            return None
    
    return None


def check_user_comment(content_type: str, owner_id: int, item_id: int, user_id: int) -> tuple:
    try:
        comment = None
        
        if content_type == 'post':
            comment = find_comment_in_post(owner_id, item_id, user_id)
        elif content_type == 'photo':
            comment = find_comment_in_photo(owner_id, item_id, user_id)
        elif content_type == 'video':
            comment = find_comment_in_video(owner_id, item_id, user_id)
        else:
            return 'error', "Неизвестный тип контента"
        
        if comment is None:
            return 'missing', "Комментарий не найден"
        
        text = comment.get('text', '').strip()
        return is_quality_comment(text)
        
    except Exception as e:
        print(f"⚠️ Ошибка проверки комментария: {e}", flush=True)
        return 'error', f"Ошибка проверки: {e}"


# ====================== ОТПРАВКА СООБЩЕНИЙ ======================

def vk_api_request(method: str, params: dict) -> dict:
    url = f"https://api.vk.com/method/{method}"
    params['v'] = VK_API_VERSION
    params['access_token'] = GROUP_TOKEN
    
    try:
        response = requests.post(url, data=params, timeout=10)
        result = response.json()
        
        if 'error' in result:
            print(f"⚠️ Ошибка VK API: {result['error']}", flush=True)
            return {'error': result['error']}
        
        return result.get('response', {})
    except Exception as e:
        print(f"⚠️ Ошибка запроса к VK API: {e}", flush=True)
        return {'error': str(e)}


def send_message(peer_id: int, text: str) -> Optional[int]:
    global pending_deletions
    
    try:
        rate_limit()
        random_id = int(time.time() * 1000)
        
        params = {
            'peer_ids': peer_id,
            'message': text,
            'random_id': random_id,
            'group_id': GROUP_ID
        }
        
        result = vk_api_request('messages.send', params)
        
        print(f"✅ Отправлено: {text[:50]}...", flush=True)
        
        conv_msg_id = None
        
        if isinstance(result, list) and len(result) > 0:
            conv_msg_id = result[0].get('conversation_message_id')
        elif isinstance(result, dict):
            conv_msg_id = result.get('conversation_message_id')
        elif isinstance(result, int) and result != 0:
            conv_msg_id = result
        
        if conv_msg_id:
            print(f"📦 Получен conversation_message_id: {conv_msg_id}", flush=True)
            
            with deletions_lock:
                pending_deletions.append({
                    'peer_id': peer_id,
                    'conv_message_id': conv_msg_id,
                    'created_at': datetime.now()
                })
                save_bot_message(peer_id, conv_msg_id)
            print(f"✅ Сообщение будет удалено через {DELETE_AFTER} секунд", flush=True)
            return conv_msg_id
        else:
            print(f"⚠️ Не удалось получить conversation_message_id", flush=True)
            return None
            
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")
        return None


def delete_message_by_conv_id(peer_id: int, conv_message_id: int) -> bool:
    try:
        rate_limit()
        
        params = {
            'peer_id': peer_id,
            'cmids': conv_message_id,
            'delete_for_all': 1,
            'group_id': GROUP_ID
        }
        
        result = vk_api_request('messages.delete', params)
        
        if isinstance(result, dict) and 'error' in result:
            error_msg = str(result['error'])
            if 'message can not be found' in error_msg or 'message not found' in error_msg:
                print(f"⚠️ Сообщение {conv_message_id} уже не существует, удаляем запись", flush=True)
                remove_bot_message(conv_message_id)
                return True
        
        if isinstance(result, dict):
            key = f"{peer_id}_{conv_message_id}"
            if key in result and result[key] == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True
            for k, v in result.items():
                if str(conv_message_id) in k or str(peer_id) in k:
                    if v == 1:
                        print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                        return True
            if result.get('status') == 'ok' or result.get('deleted') == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True
        
        if isinstance(result, int):
            if result == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True
        
        print(f"⚠️ Не удалось удалить сообщение {conv_message_id}", flush=True)
        return False
        
    except Exception as e:
        print(f"⚠️ Ошибка удаления: {e}", flush=True)
        return False


def cleanup_worker():
    global pending_deletions
    print("🔄 Воркер удаления запущен", flush=True)
    
    while True:
        try:
            time.sleep(30)
            
            now = datetime.now()
            to_delete = []
            
            with deletions_lock:
                remaining = []
                for item in pending_deletions:
                    elapsed = (now - item['created_at']).total_seconds()
                    if elapsed >= DELETE_AFTER:
                        to_delete.append(item)
                    else:
                        remaining.append(item)
                pending_deletions = remaining
            
            for item in to_delete:
                print(f"🔍 Удаляю сообщение {item['conv_message_id']}...", flush=True)
                if delete_message_by_conv_id(item['peer_id'], item['conv_message_id']):
                    remove_bot_message(item['conv_message_id'])
        except Exception as e:
            print(f"❌ Ошибка воркера: {e}", flush=True)
            time.sleep(5)


def get_inactive_users(peer_id: int) -> str:
    global user_activity
    now = datetime.now()
    inactive = []
    
    with activity_lock:
        for user_id, data in user_activity.items():
            if data.get('last_post_time'):
                last_post = data['last_post_time']
                days_inactive = (now - last_post).days
                if days_inactive > 10:
                    inactive.append((user_id, days_inactive))
            else:
                inactive.append((user_id, 999))
    
    if not inactive:
        return "✅ Все участники активны!"
    
    text = "📋 Неактивные участники (более 10 дней без публикаций):\n\n"
    for user_id, days in inactive:
        try:
            rate_limit()
            user_info = vk_user.users.get(user_ids=[user_id])[0]
            name = f"{user_info['first_name']} {user_info['last_name']}"
            text += f"👤 {name} (ID: {user_id}) — {days} дней\n"
        except:
            text += f"👤 ID: {user_id} — {days} дней\n"
    
    return text


# ====================== КОМАНДЫ АДМИНА ======================

def handle_admin_commands(text: str, user_id: int, peer_id: int, message_id: int) -> bool:
    global vip_links, queue
    
    text_lower = text.lower().strip()
    
    if not is_owner(user_id):
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, "❌ Только владелец чата может использовать команды!")
        return True
    
    if message_id:
        delete_message_by_conv_id(peer_id, message_id)
    
    cleanup_expired_vip()
    
    if text_lower.startswith('!vip '):
        try:
            vk_link = extract_vk_link(text.split()[1])
        except IndexError:
            send_message(peer_id, "⚠️ Использование: !vip [ссылка]")
            return True
        
        if vk_link:
            with vip_links_lock:
                for vip in vip_links:
                    if vip['link'] == vk_link:
                        send_message(peer_id, f"⚠️ Ссылка уже в VIP!")
                        return True
                vip_links.append({
                    'link': vk_link,
                    'added_by': user_id,
                    'expires_at': datetime.now() + timedelta(hours=VIP_DURATION_HOURS)
                })
                save_vip_links()
                reload_vip_links()
            send_message(peer_id, f"⭐ VIP-ссылка добавлена на 24 часа!\n🔗 {make_clickable_link(vk_link)}")
        else:
            send_message(peer_id, "⚠️ Не удалось распознать ссылку!")
        return True
    
    if text_lower.startswith('!delvip'):
        parts = text.split()
        if len(parts) >= 2:
            link_to_delete = extract_vk_link(parts[1])
            if link_to_delete:
                with vip_links_lock:
                    initial_count = len(vip_links)
                    vip_links = [v for v in vip_links if v['link'] != link_to_delete]
                    removed = initial_count - len(vip_links)
                    save_vip_links()
                    reload_vip_links()
                if removed > 0:
                    send_message(peer_id, f"✅ VIP-ссылка удалена!")
                else:
                    send_message(peer_id, f"⚠️ Ссылка не найдена в VIP!")
            else:
                send_message(peer_id, "⚠️ Не удалось распознать ссылку!")
        else:
            send_message(peer_id, "⚠️ Использование: !delvip [ссылка]")
        return True
    
    if text_lower == '!vip_list':
        with vip_links_lock:
            if not vip_links:
                send_message(peer_id, "📭 VIP-ссылок нет")
                return True
            result = "⭐ VIP-ссылки:\n\n"
            now = datetime.now()
            for vip in vip_links:
                remaining = vip['expires_at'] - now
                hours = int(remaining.total_seconds() // 3600)
                result += f"🔗 {make_clickable_link(vip['link'])}\n⏳ Осталось: {hours}ч\n\n"
            send_message(peer_id, result)
        return True
    
    if text_lower == '!inactive':
        inactive_text = get_inactive_users(peer_id)
        send_message(peer_id, inactive_text)
        return True
    
    if text_lower.startswith('!delqueue'):
        parts = text.split()
        if len(parts) >= 2:
            link_to_delete = extract_vk_link(parts[1])
            if link_to_delete:
                with queue_lock:
                    initial_count = len(queue)
                    new_queue = []
                    for item in queue:
                        item_link = item['link']
                        if item_link.startswith('http'):
                            extracted = extract_vk_link(item_link)
                            if extracted:
                                item_link = extracted
                        if item_link != link_to_delete:
                            new_queue.append(item)
                    
                    removed_count = initial_count - len(new_queue)
                    queue = new_queue
                    save_queue()
                    reload_queue()
                
                if removed_count > 0:
                    send_message(peer_id, f"✅ Ссылка удалена из очереди ({removed_count} шт.)!")
                else:
                    send_message(peer_id, f"⚠️ Ссылка не найдена в очереди!")
            else:
                send_message(peer_id, "⚠️ Не удалось распознать ссылку!")
        else:
            send_message(peer_id, "⚠️ Использование: !delqueue [ссылка]")
        return True
    
    if text_lower == '!clearqueue':
        with queue_lock:
            count = len(queue)
            queue = []
            save_queue()
            reload_queue()
        send_message(peer_id, f"✅ Очередь полностью очищена! (удалено {count} ссылок)")
        return True
    
    if text_lower == '!queue_list':
        with queue_lock:
            if not queue:
                send_message(peer_id, "📭 Очередь пустая")
                return True
            result = "📋 Очередь ссылок:\n\n"
            for i, item in enumerate(queue, 1):
                result += f"{i}. 🔗 {make_clickable_link(item['link'])}\n"
            send_message(peer_id, result)
        return True
    
    return False


# ====================== ОСНОВНАЯ ЛОГИКА ======================

def can_user_post(user_id: int) -> bool:
    global queue
    with queue_lock:
        user_posts = [i for i, item in enumerate(queue) if item['user_id'] == user_id]
        if not user_posts:
            return True
        return len(queue) - user_posts[-1] - 1 >= 5


def get_posts_after_user(user_id: int) -> int:
    global queue
    with queue_lock:
        user_posts = [i for i, item in enumerate(queue) if item['user_id'] == user_id]
        if not user_posts:
            return 0
        return len(queue) - user_posts[-1] - 1


def process_message(peer_id: int, user_id: int, text: str, message_id: int, event_id: str = ""):
    global queue
    
    print(f"\n📩 {user_id}: {text[:80]}", flush=True)
    sys.stdout.flush()
    
    if user_id < 0:
        return
    
    text_lower = text.lower().strip()
    
    command_prefixes = ['!vip', '!delvip', '!inactive', '!delqueue', '!clearqueue', '!queue_list']
    is_command = any(text_lower.startswith(cmd) for cmd in command_prefixes)
    
    if is_command:
        handle_admin_commands(text, user_id, peer_id, message_id)
        return
    
    mention = get_mention(user_id)
    
    if is_owner(user_id):
        parsed = parse_content_link(text)
        if parsed:
            content_type, owner_id, item_id = parsed
            vk_link = extract_vk_link(text)
            
            with queue_lock:
                queue.append({
                    'link': vk_link,
                    'user_id': user_id,
                    'timestamp': datetime.now(),
                    'is_owner_post': 0
                })
                if len(queue) > MAX_QUEUE_SIZE:
                    queue.pop(0)
                save_queue()
            send_message(peer_id, f"{mention}, ✅ ваша ссылка опубликована!\n🔗 {make_clickable_link(vk_link)}")
            return
        else:
            return
    
    vk_link = extract_vk_link(text)
    
    if not vk_link:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 сообщение должно содержать только ссылку на контент!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return
    
    text_without_link = text
    text_without_link = re.sub(r'https?://(m\.)?vk\.(com|ru)/' + re.escape(vk_link) + r'(\?[^\s]*)?', '', text_without_link)
    text_without_link = re.sub(r'(m\.)?vk\.(com|ru)/' + re.escape(vk_link) + r'(\?[^\s]*)?', '', text_without_link)
    text_without_link = text_without_link.replace(vk_link, '')
    text_without_link = text_without_link.strip()
    
    if text_without_link:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 сообщение должно содержать ТОЛЬКО ссылку на контент!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return
    
    parsed = parse_content_link(vk_link)
    if not parsed:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 не удалось распознать ссылку!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return
    
    content_type, owner_id, item_id = parsed
    
    can_comment, reason = can_comment_on_content(content_type, owner_id, item_id)
    if not can_comment:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, ⚠️ невозможно проверить комментарии!\n\n📌 {reason}\n\n💡 Публикуем только контент с ОТКРЫТЫМИ комментариями.\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return
    
    if not can_user_post(user_id):
        need = max(0, 5 - get_posts_after_user(user_id))
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, ⏳ ждем Вас через {need} ссылок!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return
    
    cleanup_expired_vip()
    
    missing_vip = []
    with vip_links_lock:
        for vip in vip_links:
            parsed = parse_content_link(vip['link'])
            if not parsed:
                continue
            content_type, owner_id, item_id = parsed
            status, reason = check_user_comment(content_type, owner_id, item_id, user_id)
            if status != 'ok':
                missing_vip.append((vip, reason))
    
    if missing_vip:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        text = f"{mention}, ⭐ обязательно оставь качественные комментарии под VIP-ссылками:\n\n"
        text += "❗️ Требования к комментарию:\n"
        text += "• Отдельный комментарий (НЕ ответ на чужой)\n"
        text += "• Минимум 10 символов\n"
        text += "• Только текст (без смайликов и стикеров)\n"
        text += "• Осмысленный текст\n\n"
        for vip, reason in missing_vip:
            text += f"⭐ {make_clickable_link(vip['link'])}\n   ❌ {reason}\n\n"
        text += f"{'─' * 30}\n"
        text += "⏳ На выполнение даётся 5 минут!\n"
        text += "✅ После того, как оставишь комментарии, отправь свою ссылку снова.\n\n"
        text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
        send_message(peer_id, text)
        return
    
    with queue_lock:
        regular_links = [item for item in queue[-5:]]
    
    if regular_links:
        missing_regular = []
        for item in regular_links:
            parsed = parse_content_link(item['link'])
            if not parsed:
                continue
            content_type, owner_id, item_id = parsed
            status, reason = check_user_comment(content_type, owner_id, item_id, user_id)
            if status != 'ok':
                missing_regular.append((item, reason))
        
        if missing_regular:
            if message_id:
                delete_message_by_conv_id(peer_id, message_id)
            text = f"{mention}, 📋 обязательно оставь качественные комментарии под этими ссылками:\n\n"
            text += "❗️ Требования к комментарию:\n"
            text += "• Отдельный комментарий (НЕ ответ на чужой)\n"
            text += "• Минимум 10 символов\n"
            text += "• Только текст (без смайликов и стикеров)\n"
            text += "• Осмысленный текст\n\n"
            for item, reason in missing_regular:
                text += f"▫️ {make_clickable_link(item['link'])}\n   ❌ {reason}\n\n"
            text += f"{'─' * 30}\n"
            text += "⏳ На выполнение даётся 5 минут!\n"
            text += "✅ После того, как оставишь комментарии, отправь свою ссылку снова.\n\n"
            text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
            send_message(peer_id, text)
            return
    
    with queue_lock:
        queue.append({
            'link': vk_link,
            'user_id': user_id,
            'timestamp': datetime.now(),
            'is_owner_post': 0
        })
        if len(queue) > MAX_QUEUE_SIZE:
            queue.pop(0)
        save_queue()
    
    with activity_lock:
        user_activity[user_id] = {
            'last_post_time': datetime.now(),
            'post_count': user_activity.get(user_id, {}).get('post_count', 0) + 1
        }
    save_user_activity(user_id)
    
    text = f"{mention}, ✅ ваша ссылка опубликована!\n🔗 {make_clickable_link(vk_link)}\n📊 В очереди: {len(queue)}\n\n"
    text += "⏳ Ждем Вас через 5 ссылок!\n\n"
    text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
    send_message(peer_id, text)
    print(f"   ✅ Опубликовано!", flush=True)


# ====================== CALLBACK API ======================

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = CONFIRMATION_CODE.encode() if self.path in ['/', '/callback'] else b'Bot is running'
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        
        try:
            data = json.loads(body)
            event_type = data.get('type', '')
            print(f"📥 Событие: {event_type}", flush=True)
            
            if event_type == 'confirmation':
                rb = CONFIRMATION_CODE.encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)
            
            elif event_type == 'message_new':
                msg = data.get('object', {}).get('message', {})
                
                action = msg.get('action', {})
                if action and action.get('type') in ['chat_invite_user', 'chat_invite_user_by_link']:
                    rb = b'ok'
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/plain')
                    self.send_header('Content-Length', str(len(rb)))
                    self.end_headers()
                    self.wfile.write(rb)
                    return
                
                event_id = data.get('event_id', '')
                thread = threading.Thread(target=process_message, args=(
                    msg.get('peer_id', 0),
                    msg.get('from_id', 0),
                    msg.get('text', ''),
                    msg.get('conversation_message_id', msg.get('id', 0)),
                    event_id
                ), daemon=True)
                thread.start()
                
                rb = b'ok'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)
            
            else:
                rb = b'ok'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)
        except Exception as e:
            print(f"❌ Ошибка: {e}", flush=True)
            rb = b'ok'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(rb)))
            self.end_headers()
            self.wfile.write(rb)
    
    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_database()
    load_data()
    cleanup_old_queue()
    
    vk_group_session = vk_api.VkApi(token=GROUP_TOKEN)
    vk_group = vk_group_session.get_api()
    print("✅ Групповой API подключен", flush=True)
    
    vk_user_session = vk_api.VkApi(token=USER_TOKEN)
    vk_user = vk_user_session.get_api()
    print("✅ Пользовательский API подключен", flush=True)
    
    cleanup_thread = threading.Thread(target=cleanup_worker)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    print("✅ Воркер удаления запущен", flush=True)
    
    print(f"📡 Порт: {PORT}", flush=True)
    sys.stdout.flush()
    
    server = HTTPServer(('0.0.0.0', PORT), CallbackHandler)
    print(f"✅ Сервер запущен на 0.0.0.0:{PORT}", flush=True)
    sys.stdout.flush()
    server.serve_forever()
