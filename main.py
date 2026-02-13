import os
import sqlite3
import asyncio
import secrets
import string
from typing import Optional
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice
from aiogram.utils.keyboard import InlineKeyboardBuilder

API_TOKEN = "8540329103:AAFZiF1lDJ5zxvik9YHkEBP0iXdH_Nr_LsM"
ADMIN_IDS = [7521290887]  # Замени на свой Telegram ID

class States(StatesGroup):
    creating_folder = State()
    adding_user = State()
    giving_permission = State()
    waiting_file = State()
    renaming_file = State()
    creating_link_name = State()
    creating_link_permission = State()
    admin_creating_tariff = State()
    admin_tariff_name = State()
    admin_tariff_files = State()
    admin_tariff_folders = State()
    admin_tariff_price = State()
    admin_find_user = State()
    admin_assign_tariff = State()
    admin_edit_tariff_name = State()
    admin_edit_tariff_files = State()
    admin_edit_tariff_folders = State()
    admin_edit_tariff_price = State()

class DB:
    def __init__(self):
        self.conn = sqlite3.connect("bot.db", check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_tables()

    def _init_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                tg_id INTEGER UNIQUE
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INTEGER,
                owner_id INTEGER NOT NULL,
                access_type TEXT DEFAULT 'private',
                position INTEGER DEFAULT 0,
                FOREIGN KEY(owner_id) REFERENCES users(id)
            )
        """)
        # Добавить колонку position если её нет
        try:
            self.cursor.execute("ALTER TABLE folders ADD COLUMN position INTEGER DEFAULT 0")
        except:
            pass
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS folder_permissions (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                permission TEXT,
                notify_on_changes INTEGER DEFAULT 0,
                FOREIGN KEY(folder_id) REFERENCES folders(id),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        # Добавить колонку notify_on_changes если её нет
        try:
            self.cursor.execute("ALTER TABLE folder_permissions ADD COLUMN notify_on_changes INTEGER DEFAULT 0")
        except:
            pass
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS access_requests (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                target_tg_id INTEGER NOT NULL,
                permission TEXT,
                FOREIGN KEY(folder_id) REFERENCES folders(id)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS access_links (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                token TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                permission TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(folder_id) REFERENCES folders(id)
            )
        """)
        # Добавить колонку name если её нет
        try:
            self.cursor.execute("ALTER TABLE access_links ADD COLUMN name TEXT NOT NULL DEFAULT 'Ссылка'")
        except:
            pass
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_type TEXT DEFAULT 'document',
                position INTEGER DEFAULT 0,
                FOREIGN KEY(folder_id) REFERENCES folders(id)
            )
        """)
        # Добавить колонку position если её нет
        try:
            self.cursor.execute("ALTER TABLE files ADD COLUMN position INTEGER DEFAULT 0")
        except:
            pass
        
        # Добавить колонку file_id если её нет
        try:
            self.cursor.execute("ALTER TABLE files ADD COLUMN file_id TEXT")
        except:
            pass
        
        # Добавить колонку file_type если её нет
        try:
            self.cursor.execute("ALTER TABLE files ADD COLUMN file_type TEXT DEFAULT 'document'")
        except:
            pass
        
        # Добавить колонку file_size если её нет
        try:
            self.cursor.execute("ALTER TABLE files ADD COLUMN file_size INTEGER DEFAULT 0")
        except:
            pass
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS link_users (
                id INTEGER PRIMARY KEY,
                token TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(token, user_id),
                FOREIGN KEY(token) REFERENCES access_links(token) ON DELETE CASCADE
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tariffs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                storage_limit INTEGER,
                file_count_limit INTEGER,
                folder_count_limit INTEGER,
                price_stars INTEGER,
                description TEXT
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                tariff_id INTEGER NOT NULL,
                subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(tariff_id) REFERENCES tariffs(id)
            )
        """)
        
        # Инициализируем стандартные тарифы
        self.cursor.execute("SELECT COUNT(*) FROM tariffs")
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute("""
                INSERT INTO tariffs (name, storage_limit, file_count_limit, folder_count_limit, price_stars, description)
                VALUES 
                ('Бесплатный', 1, 10, 2, 0, 'Ограниченный тариф'),
                ('Базовый', 10, 100, 10, 49, 'Увеличенные лимиты в месяц'),
                ('Премиум', 100, 1000, 50, 99, 'Максимальные возможности в месяц')
            """)
        
        self.conn.commit()

    def get_or_create_user(self, tg_id: int) -> int:
        self.cursor.execute("SELECT id FROM users WHERE tg_id = ?", (tg_id,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        self.cursor.execute("INSERT INTO users (tg_id) VALUES (?)", (tg_id,))
        self.conn.commit()
        return self.cursor.lastrowid

    def create_folder(self, name: str, parent_id: Optional[int], owner_id: int) -> int:
        self.cursor.execute(
            "INSERT INTO folders (name, parent_id, owner_id) VALUES (?, ?, ?)",
            (name, parent_id, owner_id)
        )
        self.conn.commit()
        return self.cursor.lastrowid

    def get_folder(self, folder_id: int) -> dict:
        self.cursor.execute(
            "SELECT id, name, parent_id, owner_id, access_type, position FROM folders WHERE id = ?",
            (folder_id,)
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "id": row[0], "name": row[1], "parent_id": row[2],
                "owner_id": row[3], "access_type": row[4], "position": row[5]
            }
        return None

    def get_root_folder(self, owner_id: int) -> Optional[int]:
        self.cursor.execute(
            "SELECT id FROM folders WHERE parent_id IS NULL AND owner_id = ? LIMIT 1",
            (owner_id,)
        )
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return None

    def get_contents(self, folder_id: int) -> tuple:
        if folder_id is None:
            return [], []
        self.cursor.execute(
            "SELECT id, name FROM folders WHERE parent_id = ? ORDER BY position ASC", (folder_id,)
        )
        folders = self.cursor.fetchall()
        self.cursor.execute(
            "SELECT id, filename FROM files WHERE folder_id = ? ORDER BY position ASC", (folder_id,)
        )
        files = self.cursor.fetchall()
        return folders, files

    def get_full_tree(self, folder_id: int, indent: str = "") -> str:
        """Получить всё содержимое папки рекурсивно в виде дерева"""
        if folder_id is None:
            return ""
        
        tree = ""
        
        # Получить подпапки
        self.cursor.execute(
            "SELECT id, name FROM folders WHERE parent_id = ? ORDER BY position ASC", (folder_id,)
        )
        folders = self.cursor.fetchall()
        
        # Получить файлы
        self.cursor.execute(
            "SELECT id, filename FROM files WHERE folder_id = ? ORDER BY position ASC", (folder_id,)
        )
        files = self.cursor.fetchall()
        
        # Добавить папки
        for f_id, f_name in folders:
            tree += f"{indent}📁 {f_name}\n"
            # Рекурсивно получить содержимое подпапки
            tree += self.get_full_tree(f_id, indent + "  ")
        
        # Добавить файлы
        for file_id, filename in files:
            tree += f"{indent}📄 {filename}\n"
        
        return tree

    def get_folder_path(self, folder_id: int) -> str:
        """Получить полный путь до папки: Корень => Папка 1 => Папка 2"""
        path_parts = []
        current_id = folder_id
        
        while current_id is not None:
            folder = self.get_folder(current_id)
            if not folder:
                break
            path_parts.insert(0, folder['name'])
            current_id = folder['parent_id']
        
        return " => ".join(path_parts)

    def get_file_path(self, file_id: int) -> str:
        """Получить полный путь до файла: Корень => Папка 1 => file.txt"""
        file = self.get_file(file_id)
        if not file:
            return ""
        
        folder_path = self.get_folder_path(file['folder_id'])
        if folder_path:
            return f"{folder_path} => {file['filename']}"
        return file['filename']

    def get_available_folders(self, user_id: int) -> list:
        # Собственные корневые папки
        self.cursor.execute(
            "SELECT id, name, owner_id FROM folders WHERE parent_id IS NULL AND owner_id = ? "
            "UNION "
            "SELECT DISTINCT f.id, f.name, f.owner_id FROM folders f "
            "JOIN folder_permissions fp ON f.id = fp.folder_id "
            "JOIN users u ON fp.user_id = u.id "
            "WHERE u.tg_id = ? AND f.parent_id IS NULL",
            (user_id, user_id)
        )
        return self.cursor.fetchall()

    def get_shared_folders(self, user_id: int) -> list:
        # Только папки, к которым дан доступ (не собственные)
        # user_id это telegram_id, нужно получить user id из базы
        self.cursor.execute(
            "SELECT id FROM users WHERE tg_id = ?", (user_id,)
        )
        user_row = self.cursor.fetchone()
        if not user_row:
            return []
        
        db_user_id = user_row[0]
        self.cursor.execute(
            "SELECT DISTINCT f.id, f.name, f.owner_id FROM folders f "
            "JOIN folder_permissions fp ON f.id = fp.folder_id "
            "WHERE fp.user_id = ? AND f.owner_id != ?",
            (db_user_id, db_user_id)
        )
        return self.cursor.fetchall()

    def generate_access_token(self, folder_id: int, permission: str, name: str = "Ссылка") -> str:
        """Сгенерировать уникальный токен для доступа и сохранить в БД"""
        # Генерируем случайный токен из букв и цифр
        token = secrets.token_urlsafe(16)
        
        self.cursor.execute(
            "INSERT INTO access_links (folder_id, token, permission, name) VALUES (?, ?, ?, ?)",
            (folder_id, token, permission, name)
        )
        self.conn.commit()
        return token

    def get_access_by_token(self, token: str) -> dict:
        """Получить информацию о доступе по токену"""
        self.cursor.execute(
            "SELECT id, folder_id, permission FROM access_links WHERE token = ?",
            (token,)
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "id": row[0], "folder_id": row[1], "permission": row[2]
            }
        return None

    def delete_access_token(self, token: str):
        """Удалить токен доступа"""
        self.cursor.execute("DELETE FROM access_links WHERE token = ?", (token,))
        self.conn.commit()

    def track_link_usage(self, token: str, user_id: int, tg_id: int):
        """Отследить использование ссылки пользователем"""
        try:
            self.cursor.execute(
                "INSERT OR IGNORE INTO link_users (token, user_id, tg_id) VALUES (?, ?, ?)",
                (token, user_id, tg_id)
            )
            self.conn.commit()
        except:
            pass

    def get_link_users(self, token: str) -> list:
        """Получить список пользователей, которые использовали эту ссылку"""
        self.cursor.execute(
            "SELECT lu.tg_id, lu.used_at FROM link_users lu "
            "WHERE lu.token = ? ORDER BY lu.used_at DESC",
            (token,)
        )
        return self.cursor.fetchall()

    def get_all_links_for_folder(self, folder_id: int) -> list:
        """Получить все ссылки доступа для папки"""
        self.cursor.execute(
            "SELECT id, token, name, permission, created_at FROM access_links WHERE folder_id = ? ORDER BY created_at DESC",
            (folder_id,)
        )
        return self.cursor.fetchall()

    def get_all_link_users(self, folder_id: int) -> list:
        """Получить всех пользователей которые получили доступ через все ссылки папки"""
        self.cursor.execute(
            "SELECT DISTINCT lu.tg_id, lu.used_at FROM link_users lu "
            "JOIN access_links al ON lu.token = al.token "
            "WHERE al.folder_id = ? "
            "ORDER BY lu.used_at DESC",
            (folder_id,)
        )
        return self.cursor.fetchall()

    def check_access(self, user_id: int, folder_id: int, permission: str = "read") -> bool:
        folder = self.get_folder(folder_id)
        if not folder:
            return False
        if folder["owner_id"] == user_id:
            return True
        if folder["access_type"] == "public":
            return True
        if folder["access_type"] == "restricted":
            self.cursor.execute(
                "SELECT permission FROM folder_permissions WHERE folder_id = ? AND user_id = ?",
                (folder_id, user_id)
            )
            result = self.cursor.fetchone()
            if result and (result[0] == permission or result[0] == "write"):
                return True
        return False

    def can_view_folder(self, user_id: int, folder_id: int) -> bool:
        """Проверить, может ли пользователь видеть папку (для отображения в списке)"""
        # Если владелец - может видеть
        if self.check_access(user_id, folder_id):
            return True
        
        # Если подпапка папки, к которой есть доступ - может видеть
        folder = self.get_folder(folder_id)
        if not folder or folder["parent_id"] is None:
            return False
        
        # Проверить доступ к родительской папке
        return self.check_access(user_id, folder["parent_id"])

    def can_access_file(self, user_id: int, file_id: int) -> bool:
        """Проверить, может ли пользователь открыть файл (через иерархию папок)"""
        file = self.get_file(file_id)
        if not file:
            return False
        
        # Проверить доступ к папке, в которой находится файл
        folder_id = file["folder_id"]
        
        # Проверить прямой доступ или доступ через родительскую папку
        while folder_id is not None:
            if self.check_access(user_id, folder_id):
                return True
            
            folder = self.get_folder(folder_id)
            if not folder:
                return False
            folder_id = folder["parent_id"]
        
        return False

    def can_access_file_write(self, user_id: int, file_id: int) -> bool:
        """Проверить, может ли пользователь изменять файл (через иерархию папок)"""
        file = self.get_file(file_id)
        if not file:
            return False
        
        # Проверить разрешение на запись в папке, в которой находится файл
        folder_id = file["folder_id"]
        
        # Проверить через иерархию папок
        while folder_id is not None:
            if self.check_access(user_id, folder_id, permission="write"):
                return True
            
            folder = self.get_folder(folder_id)
            if not folder:
                return False
            folder_id = folder["parent_id"]
        
        return False

    def get_root_shared_folder(self, user_id: int, folder_id: int) -> Optional[int]:
        """Найти корневую папку, к которой дан доступ этому пользователю"""
        self.cursor.execute(
            "SELECT id FROM users WHERE id = ?", (user_id,)
        )
        if not self.cursor.fetchone():
            return None
        
        # Проверить, есть ли прямой доступ к этой папке
        self.cursor.execute(
            "SELECT folder_id FROM folder_permissions WHERE folder_id = ? AND user_id = ?",
            (folder_id, user_id)
        )
        if self.cursor.fetchone():
            return folder_id
        
        # Найти родителя с прямым доступом
        current_id = folder_id
        while current_id is not None:
            folder = self.get_folder(current_id)
            if not folder:
                break
            
            self.cursor.execute(
                "SELECT folder_id FROM folder_permissions WHERE folder_id = ? AND user_id = ?",
                (current_id, user_id)
            )
            if self.cursor.fetchone():
                return current_id
            
            current_id = folder["parent_id"]
        
        return None

    def add_permission(self, folder_id: int, user_id: int, permission: str, notify_on_changes: bool = False):
        self.cursor.execute(
            "INSERT OR REPLACE INTO folder_permissions (folder_id, user_id, permission, notify_on_changes) VALUES (?, ?, ?, ?)",
            (folder_id, user_id, permission, 1 if notify_on_changes else 0)
        )
        self.cursor.execute(
            "UPDATE folders SET access_type = 'restricted' WHERE id = ?", (folder_id,)
        )
        self.conn.commit()

    def remove_permission(self, folder_id: int, user_id: int):
        self.cursor.execute(
            "DELETE FROM folder_permissions WHERE folder_id = ? AND user_id = ?",
            (folder_id, user_id)
        )
        self.conn.commit()

    def get_permissions(self, folder_id: int) -> list:
        self.cursor.execute(
            "SELECT folder_permissions.id, users.tg_id, folder_permissions.permission FROM folder_permissions "
            "JOIN users ON folder_permissions.user_id = users.id WHERE folder_id = ?",
            (folder_id,)
        )
        return self.cursor.fetchall()

    def create_access_request(self, folder_id: int, target_tg_id: int, permission: str) -> int:
        self.cursor.execute(
            "INSERT INTO access_requests (folder_id, target_tg_id, permission) VALUES (?, ?, ?)",
            (folder_id, target_tg_id, permission)
        )
        self.conn.commit()
        return self.cursor.lastrowid

    def get_access_request(self, request_id: int) -> dict:
        self.cursor.execute(
            "SELECT id, folder_id, target_tg_id, permission FROM access_requests WHERE id = ?",
            (request_id,)
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "id": row[0], "folder_id": row[1],
                "target_tg_id": row[2], "permission": row[3]
            }
        return None

    def accept_access(self, request_id: int, notify_on_changes: bool = False):
        req = self.get_access_request(request_id)
        if req:
            target_user = self.get_or_create_user(req["target_tg_id"])
            self.add_permission(req["folder_id"], target_user, req["permission"], notify_on_changes)
            self.cursor.execute("DELETE FROM access_requests WHERE id = ?", (request_id,))
            self.conn.commit()

    def get_notification_status(self, folder_id: int, user_id: int) -> bool:
        """Получить статус уведомлений для пользователя в папке"""
        self.cursor.execute(
            "SELECT notify_on_changes FROM folder_permissions WHERE folder_id = ? AND user_id = ?",
            (folder_id, user_id)
        )
        result = self.cursor.fetchone()
        if result:
            return bool(result[0])
        return False

    def toggle_notification(self, folder_id: int, user_id: int):
        """Переключить статус уведомлений для пользователя в папке"""
        current = self.get_notification_status(folder_id, user_id)
        self.cursor.execute(
            "UPDATE folder_permissions SET notify_on_changes = ? WHERE folder_id = ? AND user_id = ?",
            (1 if not current else 0, folder_id, user_id)
        )
        self.conn.commit()
        return not current

    def get_users_to_notify(self, folder_id: int) -> list:
        """Получить список пользователей с включенными уведомлениями для папки и её родителей"""
        users_to_notify = set()
        
        current_id = folder_id
        while current_id is not None:
            # Получить всех пользователей с включенными уведомлениями для этой папки
            self.cursor.execute(
                "SELECT u.tg_id FROM folder_permissions fp "
                "JOIN users u ON fp.user_id = u.id "
                "WHERE fp.folder_id = ? AND fp.notify_on_changes = 1",
                (current_id,)
            )
            results = self.cursor.fetchall()
            for (tg_id,) in results:
                users_to_notify.add(tg_id)
            
            # Перейти к родительской папке
            folder = self.get_folder(current_id)
            current_id = folder["parent_id"] if folder else None
        
        return list(users_to_notify)

    def reject_access(self, request_id: int):
        self.cursor.execute("DELETE FROM access_requests WHERE id = ?", (request_id,))
        self.conn.commit()

    def save_file(self, folder_id: int, filename: str, file_id: str, file_type: str = "document", file_size: int = 0):
        # Получить максимальную позицию в этой папке
        self.cursor.execute(
            "SELECT MAX(position) FROM files WHERE folder_id = ?", (folder_id,)
        )
        max_pos = self.cursor.fetchone()[0] or 0
        
        self.cursor.execute(
            "INSERT INTO files (folder_id, filename, file_id, file_type, position, file_size) VALUES (?, ?, ?, ?, ?, ?)",
            (folder_id, filename, file_id, file_type, max_pos + 1, file_size)
        )
        self.conn.commit()

    def get_unique_filename(self, folder_id: int, filename: str) -> str:
        """Получить уникальное имя файла, если оно уже существует добавить (цифра)"""
        base_name = filename
        name, ext = os.path.splitext(filename)
        counter = 1
        
        while True:
            self.cursor.execute(
                "SELECT id FROM files WHERE folder_id = ? AND filename = ?",
                (folder_id, filename)
            )
            if not self.cursor.fetchone():
                return filename
            
            filename = f"{name}({counter}){ext}"
            counter += 1

    def rename_file(self, file_id: int, new_filename: str) -> bool:
        """Переименовать файл"""
        file = self.get_file(file_id)
        if not file:
            return False
        
        self.cursor.execute(
            "UPDATE files SET filename = ? WHERE id = ?",
            (new_filename, file_id)
        )
        self.conn.commit()
        return True
        return self.cursor.lastrowid

    def get_file(self, file_id: int) -> dict:
        self.cursor.execute(
            "SELECT id, folder_id, filename, file_id, file_type, position FROM files WHERE id = ?", (file_id,)
        )
        row = self.cursor.fetchone()
        if row:
            return {"id": row[0], "folder_id": row[1], "filename": row[2], "file_id": row[3], "file_type": row[4], "position": row[5]}
        return None

    def get_folder_files(self, folder_id: int) -> list:
        self.cursor.execute(
            "SELECT id, filename FROM files WHERE folder_id = ?", (folder_id,)
        )
        return self.cursor.fetchall()

    def delete_file(self, file_id: int):
        self.cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
        self.conn.commit()

    def delete_folder_recursive(self, folder_id: int) -> bool:
        """Удалить папку со всем содержимым (файлы и подпапки)"""
        folder = self.get_folder(folder_id)
        if not folder:
            return False
        
        # Получить все файлы в этой папке и удалить их
        self.cursor.execute("SELECT id FROM files WHERE folder_id = ?", (folder_id,))
        files = self.cursor.fetchall()
        for (file_id,) in files:
            self.delete_file(file_id)
        
        # Получить все подпапки и удалить их рекурсивно
        self.cursor.execute("SELECT id FROM folders WHERE parent_id = ?", (folder_id,))
        subfolders = self.cursor.fetchall()
        for (subfolder_id,) in subfolders:
            self.delete_folder_recursive(subfolder_id)
        
        # Удалить все разрешения для этой папки
        self.cursor.execute("DELETE FROM folder_permissions WHERE folder_id = ?", (folder_id,))
        
        # Удалить все запросы доступа для этой папки
        self.cursor.execute("DELETE FROM access_requests WHERE folder_id = ?", (folder_id,))
        
        # Удалить саму папку
        self.cursor.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
        self.conn.commit()
        
        return True

    def set_access_type(self, folder_id: int, access_type: str):
        self.cursor.execute(
            "UPDATE folders SET access_type = ? WHERE id = ?",
            (access_type, folder_id)
        )
        self.conn.commit()

    def move_folder_up(self, folder_id: int):
        folder = self.get_folder(folder_id)
        if not folder:
            return
        
        # Найти предыдущий элемент с меньшей позицией
        self.cursor.execute(
            "SELECT id, position FROM folders WHERE parent_id = ? AND position < ? ORDER BY position DESC LIMIT 1",
            (folder["parent_id"], folder.get("position", 0))
        )
        prev = self.cursor.fetchone()
        
        if prev:
            prev_id, prev_pos = prev
            curr_pos = folder.get("position", 0)
            
            # Обновить position для обоих
            self.cursor.execute("UPDATE folders SET position = ? WHERE id = ?", (prev_pos, folder_id))
            self.cursor.execute("UPDATE folders SET position = ? WHERE id = ?", (curr_pos, prev_id))
            self.conn.commit()

    def move_folder_down(self, folder_id: int):
        folder = self.get_folder(folder_id)
        if not folder:
            return
        
        # Найти следующий элемент с большей позицией
        self.cursor.execute(
            "SELECT id, position FROM folders WHERE parent_id = ? AND position > ? ORDER BY position ASC LIMIT 1",
            (folder["parent_id"], folder.get("position", 0))
        )
        next_item = self.cursor.fetchone()
        
        if next_item:
            next_id, next_pos = next_item
            curr_pos = folder.get("position", 0)
            
            # Обновить position для обоих
            self.cursor.execute("UPDATE folders SET position = ? WHERE id = ?", (next_pos, folder_id))
            self.cursor.execute("UPDATE folders SET position = ? WHERE id = ?", (curr_pos, next_id))
            self.conn.commit()

    def move_file_up(self, file_id: int):
        file = self.get_file(file_id)
        if not file:
            return
        
        # Найти предыдущий файл с меньшей позицией
        self.cursor.execute(
            "SELECT id, position FROM files WHERE folder_id = ? AND position < ? ORDER BY position DESC LIMIT 1",
            (file["folder_id"], file.get("position", 0))
        )
        prev = self.cursor.fetchone()
        
        if prev:
            prev_id, prev_pos = prev
            curr_pos = file.get("position", 0)
            
            self.cursor.execute("UPDATE files SET position = ? WHERE id = ?", (prev_pos, file_id))
            self.cursor.execute("UPDATE files SET position = ? WHERE id = ?", (curr_pos, prev_id))
            self.conn.commit()

    def move_file_down(self, file_id: int):
        file = self.get_file(file_id)
        if not file:
            return
        
        # Найти следующий файл с большей позицией
        self.cursor.execute(
            "SELECT id, position FROM files WHERE folder_id = ? AND position > ? ORDER BY position ASC LIMIT 1",
            (file["folder_id"], file.get("position", 0))
        )
        next_item = self.cursor.fetchone()
        
        if next_item:
            next_id, next_pos = next_item
            curr_pos = file.get("position", 0)
            
            self.cursor.execute("UPDATE files SET position = ? WHERE id = ?", (next_pos, file_id))
            self.cursor.execute("UPDATE files SET position = ? WHERE id = ?", (curr_pos, next_id))
            self.conn.commit()

    def move_file_up_hierarchy(self, file_id: int, user_id: int) -> bool:
        """Переместить файл на уровень выше (в родительскую папку)"""
        file = self.get_file(file_id)
        if not file:
            return False
        
        current_folder = self.get_folder(file["folder_id"])
        if not current_folder or current_folder["parent_id"] is None:
            return False
        
        # Получить родительскую папку
        parent_folder = self.get_folder(current_folder["parent_id"])
        
        # Не разрешить перемещать в корень (папка с parent_id = None)
        if parent_folder and parent_folder["parent_id"] is None:
            return False
        
        # Проверить, не пытается ли переместить выше корневой папки доступа
        root_shared = self.get_root_shared_folder(user_id, file["folder_id"])
        if root_shared and current_folder["parent_id"] == root_shared:
            # Следующий уровень выше - это папка, к которой дан доступ
            # Проверить, владелец ли пользователь этой папки
            root_folder = self.get_folder(root_shared)
            if root_folder["owner_id"] != user_id:
                return False  # Не может переместить выше папки доступа
        
        # Переместить в родительскую папку
        new_parent = current_folder["parent_id"]
        self.cursor.execute("UPDATE files SET folder_id = ? WHERE id = ?", (new_parent, file_id))
        self.conn.commit()
        return True

    def move_file_to_subfolder(self, file_id: int, subfolder_id: int, user_id: int) -> bool:
        """Переместить файл в выбранную подпапку"""
        file = self.get_file(file_id)
        if not file:
            return False
        
        # Проверить, что целевая папка в пределах доступа
        root_shared = self.get_root_shared_folder(user_id, file["folder_id"])
        target_root = self.get_root_shared_folder(user_id, subfolder_id)
        
        if root_shared and target_root and root_shared == target_root:
            self.cursor.execute("UPDATE files SET folder_id = ? WHERE id = ?", (subfolder_id, file_id))
            self.conn.commit()
            return True
        
        # Если владелец своих папок - всегда разрешить
        if not root_shared:
            self.cursor.execute("UPDATE files SET folder_id = ? WHERE id = ?", (subfolder_id, file_id))
            self.conn.commit()
            return True
        
        return False

    def move_folder_up_hierarchy(self, folder_id: int) -> bool:
        """Переместить папку на уровень выше"""
        folder = self.get_folder(folder_id)
        if not folder or folder["parent_id"] is None:
            return False
        
        parent_folder = self.get_folder(folder["parent_id"])
        if not parent_folder:
            return False
        
        # Переместить в родительскую папку родителя
        new_parent = parent_folder["parent_id"]
        self.cursor.execute("UPDATE folders SET parent_id = ? WHERE id = ?", (new_parent, folder_id))
        self.conn.commit()
        return True

    def move_folder_to_subfolder(self, folder_id: int, subfolder_id: int) -> bool:
        """Переместить папку в выбранную подпапку (проверка циклов!)"""
        folder = self.get_folder(folder_id)
        target = self.get_folder(subfolder_id)
        
        if not folder or not target:
            return False
        
        # Проверка цикла - нельзя переместить папку в её подпапку
        current = target["parent_id"]
        while current is not None:
            if current == folder_id:
                return False  # Цикл найден
            parent = self.get_folder(current)
            current = parent["parent_id"] if parent else None
        
        self.cursor.execute("UPDATE folders SET parent_id = ? WHERE id = ?", (subfolder_id, folder_id))
        self.conn.commit()
        return True
    
    def get_user_tariff(self, tg_id: int) -> dict:
        """Получить текущий тариф пользователя с проверкой истечения"""
        # Сначала получить ID пользователя из БД по его Telegram ID
        self.cursor.execute("SELECT id FROM users WHERE tg_id = ?", (tg_id,))
        user_result = self.cursor.fetchone()
        if not user_result:
            # Если пользователя нет, вернуть бесплатный тариф (он будет создан позже)
            return self.get_free_tariff()
        
        user_id = user_result[0]
        
        self.cursor.execute("""
            SELECT t.id, t.name, t.storage_limit, t.file_count_limit, t.folder_count_limit, t.price_stars, us.expires_at
            FROM user_subscriptions us
            JOIN tariffs t ON us.tariff_id = t.id
            WHERE us.user_id = ? AND us.is_active = 1
            ORDER BY us.subscribed_at DESC
            LIMIT 1
        """, (user_id,))
        row = self.cursor.fetchone()
        if row:
            expires_at = row[6]
            if expires_at:
                if datetime.fromisoformat(expires_at) < datetime.now():
                    self.cursor.execute(
                        "UPDATE user_subscriptions SET is_active = 0 WHERE user_id = ? AND is_active = 1",
                        (user_id,)
                    )
                    self.conn.commit()
                    return self.get_free_tariff()
            
            return {
                "id": row[0],
                "name": row[1],
                "storage_limit": row[2],
                "file_count_limit": row[3],
                "folder_count_limit": row[4],
                "price_stars": row[5]
            }
        return self.get_free_tariff()
    
    def get_free_tariff(self) -> dict:
        """Получить бесплатный тариф"""
        self.cursor.execute("SELECT id, name, storage_limit, file_count_limit, folder_count_limit, price_stars FROM tariffs WHERE price_stars = 0 LIMIT 1")
        row = self.cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "name": row[1],
                "storage_limit": row[2],
                "file_count_limit": row[3],
                "folder_count_limit": row[4],
                "price_stars": row[5]
            }
        # Если почему-то бесплатный тариф не создан, вернуть defaults
        return {
            "id": 1,
            "name": "Бесплатный",
            "storage_limit": 1,
            "file_count_limit": 10,
            "folder_count_limit": 2,
            "price_stars": 0
        }
    
    def get_all_tariffs(self) -> list:
        """Получить все доступные тарифы"""
        self.cursor.execute("""
            SELECT id, name, storage_limit, file_count_limit, folder_count_limit, price_stars, description
            FROM tariffs ORDER BY price_stars
        """)
        tariffs = []
        for row in self.cursor.fetchall():
            tariffs.append({
                "id": row[0],
                "name": row[1],
                "storage_limit": row[2],
                "file_count_limit": row[3],
                "folder_count_limit": row[4],
                "price_stars": row[5],
                "description": row[6]
            })
        return tariffs
    
    def subscribe_user_to_tariff(self, user_id: int, tariff_id: int) -> bool:
        """Подписать пользователя на тариф (ежемесячная подписка)"""
        try:
            self.cursor.execute(
                "UPDATE user_subscriptions SET is_active = 0 WHERE user_id = ? AND is_active = 1",
                (user_id,)
            )
            
            self.cursor.execute("SELECT price_stars FROM tariffs WHERE id = ?", (tariff_id,))
            tariff_result = self.cursor.fetchone()
            
            if tariff_result and tariff_result[0] == 0:
                expires_at = None
            else:
                expires_at = (datetime.now() + timedelta(days=30)).isoformat()
            
            self.cursor.execute(
                "INSERT INTO user_subscriptions (user_id, tariff_id, expires_at) VALUES (?, ?, ?)",
                (user_id, tariff_id, expires_at)
            )
            self.conn.commit()
            return True
        except:
            return False
    
    def get_user_usage(self, user_id: int) -> dict:
        """Получить статистику использования пользователем"""
        self.cursor.execute(
            "SELECT COUNT(*) FROM folders WHERE owner_id = ? AND parent_id IS NOT NULL",
            (user_id,)
        )
        folder_count = self.cursor.fetchone()[0]
        
        self.cursor.execute(
            "SELECT COUNT(*) FROM files f JOIN folders fo ON f.folder_id = fo.id WHERE fo.owner_id = ?",
            (user_id,)
        )
        file_count = self.cursor.fetchone()[0]
        
        self.cursor.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM files f JOIN folders fo ON f.folder_id = fo.id WHERE fo.owner_id = ?",
            (user_id,)
        )
        storage_bytes = self.cursor.fetchone()[0]
        storage_used_gb = storage_bytes / (1024 * 1024 * 1024)
        
        return {
            "folder_count": folder_count,
            "file_count": file_count,
            "storage_used": storage_used_gb
        }

    def delete_tariff(self, tariff_id: int) -> bool:
        """Удалить тариф"""
        try:
            self.cursor.execute("DELETE FROM tariffs WHERE id = ?", (tariff_id,))
            self.conn.commit()
            return True
        except:
            return False

    def update_tariff(self, tariff_id: int, name: str, file_count_limit: int, folder_count_limit: int, price_stars: int) -> bool:
        """Обновить параметры тарифа"""
        try:
            self.cursor.execute(
                "UPDATE tariffs SET name = ?, file_count_limit = ?, folder_count_limit = ?, price_stars = ? WHERE id = ?",
                (name, file_count_limit, folder_count_limit, price_stars, tariff_id)
            )
            self.conn.commit()
            return True
        except:
            return False

    def find_user_by_tg_id(self, tg_id: int):
        """Найти пользователя по Telegram ID"""
        self.cursor.execute("SELECT id, tg_id FROM users WHERE tg_id = ?", (tg_id,))
        return self.cursor.fetchone()

    def assign_tariff_to_user(self, user_id: int, tariff_id: int) -> bool:
        """Назначить пользователю тариф на месяц"""
        try:
            # Отключить старые подписки
            self.cursor.execute("UPDATE user_subscriptions SET is_active = 0 WHERE user_id = ?", (user_id,))
            
            # Создать новую подписку
            expires_at = datetime.now() + timedelta(days=30)
            self.cursor.execute(
                "INSERT INTO user_subscriptions (user_id, tariff_id, expires_at, is_active) VALUES (?, ?, ?, 1)",
                (user_id, tariff_id, expires_at)
            )
            self.conn.commit()
            return True
        except:
            return False

db = DB()
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

def add_close_and_menu_buttons(builder: InlineKeyboardBuilder) -> InlineKeyboardBuilder:
    """Добавить кнопку 'Закрыть' к клавиатуре"""
    builder.button(text="❌", callback_data="close_message")
    return builder

def build_folder_keyboard(folder_id: int, user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    folders, files = db.get_contents(folder_id)

    for f_id, f_name in folders:
        if db.can_view_folder(user_id, f_id):
            builder.button(text=f"📁 {f_name}", callback_data=f"open_folder:{f_id}")

    for file_id, filename in files:
        builder.button(text=f"📄 {filename}", callback_data=f"file_menu:{file_id}")

    builder.adjust(1)

    actions = []
    folder = db.get_folder(folder_id)
    if not folder:
        add_close_and_menu_buttons(builder)
        return builder.as_markup()
    
    # Проверить это корневая папка (parent_id = None)
    is_root = folder["parent_id"] is None
    
    # Проверить доступ к родительской папке (для проверки прав владельца на подпапку)
    has_write_access_to_parent = False
    if folder["parent_id"] is not None:
        has_write_access_to_parent = db.check_access(user_id, folder["parent_id"], permission="write")
    
    # Показать кнопку создания папки если:
    # - владелец этой папки ИЛИ
    # - имеет write доступ к этой папке ИЛИ
    # - имеет write доступ к родительской папке
    can_create_folder = (
        folder["owner_id"] == user_id or 
        db.check_access(user_id, folder_id, permission="write") or
        has_write_access_to_parent
    )
    
    if can_create_folder:
        button_text = "➕ Папка" if is_root else "➕"
        actions.append(InlineKeyboardButton(text=button_text, callback_data=f"new_folder:{folder_id}"))
    
    # Кнопка доступа если:
    # - владелец и это не корневая папка (но НЕ для корневой папки)
    can_manage_access = (
        folder["owner_id"] == user_id and not is_root
    )
    
    if can_manage_access:
        actions.append(InlineKeyboardButton(text="🔐", callback_data=f"manage_access:{folder_id}"))

    # Кнопка загрузки файла если не корневая папка И (владелец ИЛИ имеет write доступ ИЛИ write доступ к родителю)
    can_upload_file = (
        not is_root and (
            folder["owner_id"] == user_id or 
            db.check_access(user_id, folder_id, permission="write") or
            has_write_access_to_parent
        )
    )
    
    if can_upload_file:
        actions.append(InlineKeyboardButton(text="📤", callback_data=f"upload_file:{folder_id}"))

    # Кнопка удаления папки если:
    # - владелец и это не корневая папка
    can_delete_folder = (
        folder["owner_id"] == user_id and not is_root
    )
    
    if can_delete_folder:
        actions.append(InlineKeyboardButton(text="🗑️", callback_data=f"delete_folder:{folder_id}"))

    # Проверить возможность перемещения вверх
    can_go_up = folder["parent_id"] is not None
    
    # Если у пользователя есть доступ к папке (не владелец), проверить доступ к parent_id
    if can_go_up and folder["owner_id"] != user_id:
        if not db.check_access(user_id, folder["parent_id"]):
            can_go_up = False
    
    if can_go_up:
        # Если родитель - корневая папка (ID = 1), использовать my_storage
        back_callback = "my_storage" if folder['parent_id'] == 1 else f"open_folder:{folder['parent_id']}"
        actions.append(InlineKeyboardButton(text="⬅️", callback_data=back_callback))

    # Добавить кнопку уведомлений если пользователь имеет доступ (не владелец) и это не корневая папка
    if folder["owner_id"] != user_id and not is_root and db.check_access(user_id, folder_id):
        notify_status = db.get_notification_status(folder_id, user_id)
        notify_icon = "🔔" if notify_status else "🔕"
        actions.append(InlineKeyboardButton(text=notify_icon, callback_data=f"toggle_notify:{folder_id}"))

    builder.row(*actions)
    
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home"),
        InlineKeyboardButton(text="❌ Закрыть", callback_data="close_message")
    )
    return builder.as_markup()

@dp.message(Command("start"))
async def start(message: types.Message):
    telegram_id = message.from_user.id
    user_id = db.get_or_create_user(telegram_id)
    
    # Проверить если это ссылка доступа к папке по токену
    if message.text and len(message.text.split()) > 1:
        token = message.text.split(maxsplit=1)[1]
        access_info = db.get_access_by_token(token)
        
        if access_info:
            folder_id = access_info["folder_id"]
            permission = access_info["permission"]
            folder = db.get_folder(folder_id)
            
            if folder:
                # Проверить если это владелец папки
                if folder["owner_id"] == user_id:
                    await message.answer(
                        "⚠️ <b>Это ваша папка!</b>\n\n"
                        f"Папка: <b>{folder['name']}</b>\n\n"
                        "Вы не можете выдать себе доступ через ссылку.",
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(text="📁 Открыть папку", callback_data=f"open_folder:{folder_id}")
                            ]]
                        ),
                        parse_mode="HTML"
                    )
                    return
                
                # Выдать доступ пользователю
                if permission not in ["read", "write"]:
                    permission = "read"
                
                # Изменить тип доступа папки на restricted, если она была private
                if folder["access_type"] == "private":
                    db.set_access_type(folder_id, "restricted")
                
                db.cursor.execute(
                    "INSERT OR REPLACE INTO folder_permissions (folder_id, user_id, permission) VALUES (?, ?, ?)",
                    (folder_id, user_id, permission)
                )
                db.conn.commit()
                
                # Отследить использование ссылки
                db.track_link_usage(token, user_id, telegram_id)
                
                builder = InlineKeyboardBuilder()
                perm_text = "👁️ <b>Просмотр</b>" if permission == "read" else "✏️ <b>Редактирование</b>"
                builder.button(text="✅ Принять доступ", callback_data=f"confirm_access_from_link:{folder_id}:{permission}")
                builder.adjust(1)
                
                await message.answer(
                    f"🔓 <b>Доступ предоставлен!</b>\n\n"
                    f"Папка: <b>{folder['name']}</b>\n"
                    f"Права: {perm_text}\n\n"
                    f"Получать уведомления об изменениях в этой папке?",
                    reply_markup=builder.as_markup(),
                    parse_mode="HTML"
                )
                return
    
    builder = InlineKeyboardBuilder()
    
    # Кнопка для собственных папок
    builder.button(text="📁 Мое хранилище", callback_data="my_storage")
    
    # Кнопка для доступных папок (всегда показываем)
    builder.button(text="🔗 Доступные папки", callback_data="shared_folders")
    
    # Кнопка для просмотра статистики и тарифов
    builder.button(text="⭐ Мой тариф", callback_data="show_tariffs")
    
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="close_message"))
    
    await message.answer(
        "🌟 <b>ОБЛАЧНОЕ ХРАНИЛИЩЕ</b> 🌟\n\n"
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Выберите действие:\n"
        "• 📁 <b>Мое хранилище</b> - управление вашими файлами\n"
        "• 🔗 <b>Доступные папки</b> - папки, к которым вам дан доступ\n"
        "• ⭐ <b>Мой тариф</b> - просмотр тарифа и покупка премиума",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "show_tariffs")
async def show_tariffs_handler(query: types.CallbackQuery):
    """Показать доступные тарифы"""
    tariff = db.get_user_tariff(query.from_user.id)
    usage = db.get_user_usage(query.from_user.id)
    
    message_text = f"""
⭐ <b>МОЙ ТАРИФ</b>

<b>Текущий:</b> {tariff['name']} {'✓' if tariff['price_stars'] == 0 else '⭐'}
💰 <b>Цена:</b> {'Бесплатно' if tariff['price_stars'] == 0 else f"{tariff['price_stars']}⭐/месяц"}

📈 <b>ИСПОЛЬЗОВАНИЕ:</b>
• 📁 Папок: {usage['folder_count']}/{tariff['folder_count_limit']}
• 📄 Файлов: {usage['file_count']}/{tariff['file_count_limit']}

⭐ <b>ДОСТУПНЫЕ ТАРИФЫ:</b>
"""
    
    keyboard = InlineKeyboardBuilder()
    for t in db.get_all_tariffs():
        if t['price_stars'] == 0:
            keyboard.button(text=f"✓ {t['name']} (Бесплатно)", callback_data=f"buy_tariff_{t['id']}")
        else:
            keyboard.button(text=f"⭐ {t['name']} ({t['price_stars']}★/месяц)", callback_data=f"buy_tariff_{t['id']}")
    
    keyboard.button(text="🔙 Вернуться", callback_data="back_to_start")
    keyboard.adjust(1)
    
    await query.message.edit_text(message_text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await query.answer()

@dp.callback_query(F.data == "back_to_start")
async def back_to_start(query: types.CallbackQuery):
    """Вернуться на главное меню"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📁 Мое хранилище", callback_data="my_storage")
    builder.button(text="🔗 Доступные папки", callback_data="shared_folders")
    builder.button(text="⭐ Мой тариф", callback_data="show_tariffs")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="close_message"))
    
    await query.message.edit_text(
        "🌟 <b>ОБЛАЧНОЕ ХРАНИЛИЩЕ</b> 🌟\n\n"
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Выберите действие:\n"
        "• 📁 <b>Мое хранилище</b> - управление вашими файлами\n"
        "• 🔗 <b>Доступные папки</b> - папки, к которым вам дан доступ\n"
        "• ⭐ <b>Мой тариф</b> - просмотр тарифа и покупка премиума",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data == "go_home")
async def go_home(query: types.CallbackQuery):
    """Вернуться на главное меню (как после /start)"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📁 Мое хранилище", callback_data="my_storage")
    builder.button(text="🔗 Доступные папки", callback_data="shared_folders")
    builder.button(text="⭐ Мой тариф", callback_data="show_tariffs")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="close_message"))
    
    await query.message.edit_text(
        "🌟 <b>ОБЛАЧНОЕ ХРАНИЛИЩЕ</b> 🌟\n\n"
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Выберите действие:\n"
        "• 📁 <b>Мое хранилище</b> - управление вашими файлами\n"
        "• 🔗 <b>Доступные папки</b> - папки, к которым вам дан доступ\n"
        "• ⭐ <b>Мой тариф</b> - просмотр тарифа и покупка премиума",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data == "my_storage")
async def my_storage(query: types.CallbackQuery):
    db_user_id = db.get_or_create_user(query.from_user.id)
    root_folder = db.get_root_folder(db_user_id)
    
    # Если корневой папки нет - создать её
    if not root_folder:
        root_folder = db.create_folder("МОЕ ХРАНИЛИЩЕ", None, db_user_id)
    
    # Получить количество папок (не считая корневую)
    db.cursor.execute("SELECT COUNT(*) FROM folders WHERE owner_id = ? AND parent_id IS NOT NULL", (db_user_id,))
    folder_count = db.cursor.fetchone()[0]
    
    # Получить полное дерево содержимого
    tree = db.get_full_tree(root_folder)
    
    # Формировать сообщение
    message_text = f"📁 <b>МОЕ ХРАНИЛИЩЕ</b> 📁\n📊 Папок: <b>{folder_count}</b>\n\n"
    
    if tree:
        message_text += f"<b>Содержимое:</b>\n<code>{tree}</code>"
    else:
        message_text += "<i>Папка пуста. Создайте папку или загрузите файл!</i>"
    
    await query.message.edit_text(
        message_text,
        reply_markup=build_folder_keyboard(root_folder, db_user_id),
        parse_mode="HTML"
    )
    await query.answer()
    await query.answer()

@dp.callback_query(F.data == "shared_folders")
async def shared_folders(query: types.CallbackQuery):
    telegram_id = query.from_user.id
    user_id = db.get_or_create_user(telegram_id)
    shared = db.get_shared_folders(telegram_id)
    
    builder = InlineKeyboardBuilder()
    
    if not shared:
        builder.button(text="🏠 В главное меню", callback_data="go_home")
        add_close_and_menu_buttons(builder)
        await query.message.edit_text(
            "🔗 <b>ДОСТУПНЫЕ ПАПКИ</b> 🔗\n\n"
            "📭 <i>У вас нет доступа к папкам</i>\n\n"
            "Попросите владельца папки отправить вам ссылку доступа",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
        await query.answer()
        return
    
    message_text = (
        f"🔗 <b>ДОСТУПНЫЕ ПАПКИ</b> 🔗  ({len(shared)})\n\n"
    )
    
    for folder_id, folder_name, owner_id in shared:
        builder.button(text=f"📁 {folder_name}", callback_data=f"open_folder:{folder_id}")
    
    builder.button(text="🏠 В главное меню", callback_data="go_home")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)
    
    await query.message.edit_text(
        message_text + "<i>Выберите папку для открытия:</i>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("open_folder:"))
async def open_folder(query: types.CallbackQuery):
    folder_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)

    if not db.can_view_folder(db_user_id, folder_id):
        await query.answer("❌ Нет доступа", show_alert=True)
        return

    folder = db.get_folder(folder_id)
    
    # Получить полное дерево содержимого
    tree = db.get_full_tree(folder_id)
    
    # Получить количество подпапок в этой папке
    db.cursor.execute("SELECT COUNT(*) FROM folders WHERE parent_id = ?", (folder_id,))
    subfolder_count = db.cursor.fetchone()[0]
    
    # Формировать сообщение - если это не папка владельца, показываем "доступные"
    is_owner = folder["owner_id"] == db_user_id
    icon = "📁" if is_owner else "🔗"
    subtitle = "(ваша папка)" if is_owner else "(доступная папка)"
    
    #message_text = f"{icon} <b>{folder['name']}</b> {icon}\n<i>{subtitle}</i>\n📊 Папок: <b>{subfolder_count}</b>\n\n"
    message_text = f"{icon} <b>{folder['name']}</b> {icon}\n📊 Папок: <b>{subfolder_count}</b>\n\n"
    
    if tree:
        message_text += f"<b>Содержимое:</b>\n<code>{tree}</code>"
    else:
        message_text += "<i>Папка пуста</i>"
    
    await query.message.edit_text(
        message_text,
        reply_markup=build_folder_keyboard(folder_id, db_user_id),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("new_folder:"))
async def new_folder_prompt(query: types.CallbackQuery, state: FSMContext):
    try:
        parent_id_str = query.data.split(":")[1]
        parent_id = int(parent_id_str) if parent_id_str != "None" else None
    except (ValueError, IndexError):
        parent_id = None
    
    user_id = db.get_or_create_user(query.from_user.id)
    
    # Если parent_id = 0, это означает создание корневой папки
    if parent_id == 0:
        parent_id = None
    
    # Проверить write доступ (если parent_id = None, это корневая папка владельца)
    if parent_id is not None:
        folder = db.get_folder(parent_id)
        if not folder or not (db.check_access(user_id, parent_id, permission="write") or 
                folder["owner_id"] == user_id):
            await query.answer("❌ Нет прав на создание папок (требуется write доступ)", show_alert=True)
            return
    
    await state.update_data(parent_id=parent_id)
    await state.set_state(States.creating_folder)
    await query.message.edit_text("📝 Введите имя папки:")
    await query.answer()

@dp.message(States.creating_folder)
async def create_folder_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tg_id = message.from_user.id
    user_id = db.get_or_create_user(tg_id)
    parent_id = data["parent_id"]

    folder_name = message.text
    
    # Проверить лимиты тарифа
    tariff = db.get_user_tariff(tg_id)
    usage = db.get_user_usage(user_id)
    
    if not tariff:
        await message.answer("❌ Ошибка: не удалось определить ваш тариф. Попробуйте позже.")
        await state.clear()
        return
    
    if usage['folder_count'] >= tariff['folder_count_limit']:
        await message.answer(
            f"❌ <b>Лимит папок достигнут!</b>\n\n"
            f"Ваш текущий тариф: <b>{tariff['name']}</b>\n"
            f"Допустимо папок: {tariff['folder_count_limit']}\n\n"
            f"💡 Перейдите на более высокий тариф для увеличения лимита.",
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    # Создать папку
    new_folder_id = db.create_folder(folder_name, parent_id, user_id)
    
    # Отправить уведомления пользователям с включенными уведомлениями (если есть родитель)
    if parent_id is not None:
        parent_path_str = db.get_folder_path(parent_id)
        users_to_notify = db.get_users_to_notify(parent_id)
        for tg_id in users_to_notify:
            try:
                builder = InlineKeyboardBuilder()
                builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{parent_id}")
                builder.button(text="🔔 Уведомления", callback_data=f"toggle_notify:{parent_id}")
                builder.adjust(1)
                
                await bot.send_message(
                    tg_id,
                    f"📁 Папка: <b>{parent_path_str}</b>\n\n"
                    f"➕ Создана новая папка: <b>{folder_name}</b>",
                    parse_mode="HTML",
                    reply_markup=builder.as_markup()
                )
            except:
                pass  # Игнорируем ошибки отправки
    
    # Показать содержимое новосозданной папки
    message_text = f"✅ <b>Папка успешно создана!</b>\n\n<b>Имя:</b> {folder_name}"
    
    await message.answer(
        message_text,
        reply_markup=build_folder_keyboard(new_folder_id, user_id),
        parse_mode="HTML"
    )
    
    await state.clear()

@dp.callback_query(F.data.startswith("upload_file:"))
async def upload_file_prompt(query: types.CallbackQuery, state: FSMContext):
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    
    # Проверка, что это не корневая папка
    folder = db.get_folder(folder_id)
    is_root = folder["parent_id"] is None
    if is_root:
        await query.answer("❌ Нельзя загружать файлы в корневую папку", show_alert=True)
        return

    # Проверить write доступ (владелец или имеет write разрешение)
    if not (folder["owner_id"] == user_id or db.check_access(user_id, folder_id, "write")):
        await query.answer("❌ Нет прав на загрузку (требуется write доступ)", show_alert=True)
        return

    await state.update_data(folder_id=folder_id)
    await state.set_state(States.waiting_file)
    await query.message.edit_text("📤 Отправьте файл:")
    await query.answer()

@dp.message(States.waiting_file)
async def handle_file_upload(message: types.Message, state: FSMContext):
    file_id = None
    filename = None
    file_type = "document"

    if message.document:
        file_id = message.document.file_id
        filename = message.document.file_name or f"document_{file_id[:8]}"
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        filename = message.caption or "photo.jpg"
        file_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        filename = message.video.file_name or (message.caption or "video.mp4")
        file_type = "video"
    elif message.audio:
        file_id = message.audio.file_id
        filename = message.audio.file_name or (message.caption or "audio.mp3")
        file_type = "audio"
    elif message.voice:
        file_id = message.voice.file_id
        filename = message.caption or "voice.ogg"
        file_type = "voice"
    elif message.animation:
        file_id = message.animation.file_id
        filename = message.animation.file_name or (message.caption or "animation.gif")
        file_type = "animation"
    elif message.video_note:
        file_id = message.video_note.file_id
        filename = message.caption or "video_note.mp4"
        file_type = "video_note"
    else:
        await message.answer("❌ Отправьте файл, фото, видео или аудио")
        return

    data = await state.get_data()
    folder_id = data["folder_id"]
    tg_id = message.from_user.id
    db_user_id = db.get_or_create_user(tg_id)
    
    # Проверить лимиты тарифа
    tariff = db.get_user_tariff(tg_id)
    usage = db.get_user_usage(db_user_id)
    
    if not tariff:
        await message.answer("❌ Ошибка: не удалось определить ваш тариф. Попробуйте позже.")
        await state.clear()
        return
    
    # Проверка файлов
    if usage['file_count'] >= tariff['file_count_limit']:
        await message.answer(
            f"❌ <b>Лимит файлов достигнут!</b>\n\n"
            f"Ваш текущий тариф: <b>{tariff['name']}</b>\n"
            f"Допустимо файлов: {tariff['file_count_limit']}\n\n"
            f"💡 Перейдите на более высокий тариф для увеличения лимита.",
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    # Получить уникальное имя файла если оно уже существует
    filename = db.get_unique_filename(folder_id, filename)
    
    # Сохранить file_id и file_type
    db.save_file(folder_id, filename, file_id, file_type, 0)

    # Отправить уведомления пользователям с включенными уведомлениями
    folder_path_str = db.get_folder_path(folder_id)
    users_to_notify = db.get_users_to_notify(folder_id)
    for tg_id in users_to_notify:
        try:
            builder = InlineKeyboardBuilder()
            builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{folder_id}")
            builder.button(text="🔔 Уведомления", callback_data=f"toggle_notify:{folder_id}")
            builder.adjust(1)
            
            await bot.send_message(
                tg_id,
                f"📁 Папка: <b>{folder_path_str}</b>\n\n"
                f"➕ Добавлен новый файл: <b>{filename}</b>",
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
        except:
            pass  # Игнорируем ошибки отправки

    await message.answer(
        f"✅ <b>Файл успешно загружен!</b>\n\n"
        f"<b>Имя:</b> {filename}\n"
        f"<b>Тип:</b> {file_type}\n\n"
        f"Выберите действие:",
        reply_markup=build_folder_keyboard(folder_id, db_user_id),
        parse_mode="HTML"
    )
    await state.clear()

@dp.message(States.waiting_file)
async def wrong_file_upload(message: types.Message):
    await message.answer(
        "❌ <b>Неверный тип файла!</b>\n\n"
        "Пожалуйста, отправьте:\n"
        "📄 Документ\n"
        "🖼️ Фотографию\n"
        "🎬 Видео\n"
        "🎵 Аудио\n"
        "🎤 Голосовое сообщение",
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("file_menu:"))
async def file_menu(query: types.CallbackQuery):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file(db_user_id, file_id):
        await query.answer("❌ Нет доступа", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    
    # Основное действие - скачать
    builder.button(text="📥 Скачать файл", callback_data=f"download_file:{file_id}")
    
    # Редактирование если есть права
    if db.can_access_file_write(db_user_id, file_id):
        builder.button(text="✏️ Переименовать", callback_data=f"rename_file:{file_id}")
        builder.adjust(1)
        builder.button(text="⬆️ Выше", callback_data=f"move_file_up:{file_id}")
        builder.button(text="⬇️ Ниже", callback_data=f"move_file_down:{file_id}")
        builder.adjust(2)
        builder.button(text="🗑️ Удалить", callback_data=f"delete_file:{file_id}")
        builder.adjust(1)

    builder.button(text="⬅️ Назад в папку", callback_data=f"open_folder:{file['folder_id']}")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)

    file_path = db.get_file_path(file_id)
    file_type_emoji = {
        "photo": "🖼️",
        "video": "🎬",
        "audio": "🎵",
        "voice": "🎤",
        "animation": "🎞️",
        "video_note": "📹",
        "document": "📄"
    }.get(file.get("file_type", "document"), "📄")
    
    await query.message.edit_text(
        f"{file_type_emoji} <b>ФАЙЛ</b> {file_type_emoji}\n\n"
        f"<b>Имя:</b> {file['filename']}\n"
        f"<b>Путь:</b> {file_path}",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("download_file:"))
async def download_file(query: types.CallbackQuery):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file(db_user_id, file_id):
        await query.answer("❌ Нет доступа", show_alert=True)
        return

    if file["file_id"]:
        # Отправить файл в зависимости от типа
        file_type = file.get("file_type", "document")
        
        try:
            if file_type == "photo":
                await query.message.answer_photo(file["file_id"])
            elif file_type == "video":
                await query.message.answer_video(file["file_id"])
            elif file_type == "audio":
                await query.message.answer_audio(file["file_id"])
            elif file_type == "voice":
                await query.message.answer_voice(file["file_id"])
            elif file_type == "animation":
                await query.message.answer_animation(file["file_id"])
            elif file_type == "video_note":
                await query.message.answer_video_note(file["file_id"])
            else:  # document или другие типы
                await query.message.answer_document(file["file_id"])
            
            await query.answer()
        except Exception as e:
            await query.answer(f"❌ Ошибка при отправке файла: {str(e)[:50]}", show_alert=True)
    else:
        await query.answer("❌ Файл не найден", show_alert=True)

@dp.callback_query(F.data.startswith("delete_file:"))
async def delete_file(query: types.CallbackQuery):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file_write(db_user_id, file_id):
        await query.answer("❌ Нет прав", show_alert=True)
        return

    folder_id = file["folder_id"]
    filename = file["filename"]
    folder_path_str = db.get_folder_path(folder_id)
    
    db.delete_file(file_id)
    
    # Отправить уведомления пользователям с включенными уведомлениями
    users_to_notify = db.get_users_to_notify(folder_id)
    for tg_id in users_to_notify:
        try:
            builder = InlineKeyboardBuilder()
            builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{folder_id}")
            builder.button(text="🔔 Уведомления", callback_data=f"toggle_notify:{folder_id}")
            builder.adjust(1)
            
            await bot.send_message(
                tg_id,
                f"📁 Папка: <b>{folder_path_str}</b>\n\n"
                f"🗑️ Удален файл: <b>{filename}</b>",
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
        except:
            pass  # Игнорируем ошибки отправки
    
    await query.message.edit_text(
        "✅ <b>Файл успешно удален!</b>\n\n"
        "<i>Возвращение в папку...</i>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="⬅️ Назад в папку",
                    callback_data=f"open_folder:{folder_id}"
                )
            ]]
        ),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("rename_file:"))
async def rename_file_prompt(query: types.CallbackQuery, state: FSMContext):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file_write(db_user_id, file_id):
        await query.answer("❌ Нет прав", show_alert=True)
        return

    await state.update_data(file_id=file_id, folder_id=file["folder_id"])
    await state.set_state(States.renaming_file)
    await query.message.edit_text(f"📝 Введите новое имя файла:\n\nТекущее имя: {file['filename']}")
    await query.answer()

@dp.message(States.renaming_file)
async def handle_rename_file(message: types.Message, state: FSMContext):
    data = await state.get_data()
    file_id = data["file_id"]
    folder_id = data["folder_id"]
    db_user_id = db.get_or_create_user(message.from_user.id)
    
    new_filename = message.text.strip()
    
    if not new_filename:
        await message.answer("❌ Имя файла не может быть пустым")
        return
    
    # Получить исходный файл чтобы сохранить расширение
    file = db.get_file(file_id)
    old_name, ext = os.path.splitext(file["filename"])
    
    # Если новое имя не содержит расширение, добавить старое
    if not os.path.splitext(new_filename)[1]:
        new_filename = new_filename + ext
    
    # Получить уникальное имя если оно уже существует
    new_filename = db.get_unique_filename(folder_id, new_filename)
    
    if db.rename_file(file_id, new_filename):
        await message.answer(
            f"✅ <b>Файл переименован!</b>\n\n"
            f"<b>Новое имя:</b> {new_filename}",
            reply_markup=build_folder_keyboard(folder_id, db_user_id),
            parse_mode="HTML"
        )
    else:
        await message.answer("❌ <b>Ошибка при переименовании</b>\n\nПопробуйте еще раз", parse_mode="HTML")
    
    await state.clear()

@dp.callback_query(F.data.startswith("manage_access:"))
async def manage_access(query: types.CallbackQuery):
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)
    
    # Проверка, что это не корневая папка
    is_root = folder["parent_id"] is None
    if is_root:
        await query.answer("❌ Нельзя изменять доступ к корневой папке", show_alert=True)
        return

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    builder = InlineKeyboardBuilder()

    builder.button(text="📊 Мои ссылки", callback_data=f"view_links:{folder_id}")
    builder.button(text="👥 Все пользователи", callback_data=f"all_folder_users:{folder_id}")

    permissions = db.get_permissions(folder_id)

    builder.button(text="⬅️", callback_data=f"open_folder:{folder_id}")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)

    await query.message.edit_text(
        f"⚙️ Доступ к {folder['name']}",
        reply_markup=builder.as_markup()
    )
    await query.answer()


@dp.callback_query(F.data.startswith("get_link:"))
async def get_link(query: types.CallbackQuery, state: FSMContext):
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    await state.update_data(folder_id=folder_id)
    await state.set_state(States.creating_link_name)
    await query.message.edit_text("📝 Введите название для ссылки (например: 'Для коллег', 'Проект X'):")
    await query.answer()

@dp.message(States.creating_link_name)
async def create_link_name(message: types.Message, state: FSMContext):
    link_name = message.text.strip()
    
    if not link_name or len(link_name) > 50:
        await message.answer("❌ Название должно быть от 1 до 50 символов")
        return
    
    await state.update_data(link_name=link_name)
    await state.set_state(States.creating_link_permission)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="👁️ Просмотр (read)", callback_data="link_perm:read")
    builder.button(text="✏️ Редактирование (write)", callback_data="link_perm:write")
    builder.adjust(1)
    
    await message.answer( 
        f"Название: <b>{link_name}</b>\n\nВыберите тип доступа:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("link_perm:"))
async def link_permission(query: types.CallbackQuery, state: FSMContext):
    permission = query.data.split(":")[1]
    data = await state.get_data()
    folder_id = data["folder_id"]
    link_name = data["link_name"]
    
    # Создать ссылку
    token = db.generate_access_token(folder_id, permission, link_name)
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={token}"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️", callback_data=f"view_links:{folder_id}")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)
    
    await query.message.edit_text(
        f"✅ Ссылка создана!\n\n"
        f"📝 Название: <b>{link_name}</b>\n"
        f"🔐 Тип доступа: <b>{permission}</b>\n\n"
        f"🔗 Ссылка:\n<code>{link}</code>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()
    await state.clear()

@dp.callback_query(F.data.startswith("view_links:"))
async def view_links(query: types.CallbackQuery):
    """Показать все ссылки доступа для папки"""
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    links = db.get_all_links_for_folder(folder_id)
    
    if not links:
        builder = InlineKeyboardBuilder()
        builder.button(text="🔗 Создать новую ссылку", callback_data=f"get_link:{folder_id}")
        builder.button(text="⬅️", callback_data=f"manage_access:{folder_id}")
        builder.adjust(1)
        add_close_and_menu_buttons(builder)
        await query.message.edit_text(
            "🔗 Нет созданных ссылок для этой папки",
            reply_markup=builder.as_markup()
        )
        await query.answer()
        return

    builder = InlineKeyboardBuilder()
    
    for link_id, token, name, permission, created_at in links:
        link_users = db.get_link_users(token)
        user_count = len(link_users)
        builder.button(
            text=f"📝 {name}\n({permission} • {user_count} перешли)",
            callback_data=f"link_details:{folder_id}:{token}"
        )
    
    builder.button(text="➕ Новая ссылка", callback_data=f"get_link:{folder_id}")
    builder.button(text="📊 Аналитика", callback_data=f"all_link_stats:{folder_id}")
    builder.button(text="⬅️", callback_data=f"manage_access:{folder_id}")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)

    await query.message.edit_text(
        f"🔗 Ссылки доступа к папке '{folder['name']}':",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("link_details:"))
async def link_details(query: types.CallbackQuery):
    """Показать детали ссылки (кто использовал и возможность удалить)"""
    parts = query.data.split(":")
    folder_id = int(parts[1])
    token = parts[2]
    
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    access_info = db.get_access_by_token(token)
    if not access_info:
        await query.answer("❌ Ссылка не найдена", show_alert=True)
        return

    link_users = db.get_link_users(token)
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={token}"
    
    message_text = f"🔗 <b>{access_info.get('name', 'Ссылка')}</b>\n"
    message_text += f"🔐 Тип доступа: <b>{access_info['permission']}</b>\n\n"
    message_text += f"<code>{link}</code>\n\n"
    
    if link_users:
        message_text += f"👥 Пользователи, перешли по ссылке ({len(link_users)}):\n\n"
        for tg_id, used_at in link_users:
            message_text += f"👤 <a href='tg://user?id={tg_id}'>ID {tg_id}</a>\n"
            message_text += f"   ⏰ {used_at}\n\n"
    else:
        message_text += "👥 Никто ещё не использовал эту ссылку"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑️ Удалить ссылку", callback_data=f"delete_link:{folder_id}:{token}")
    builder.button(text="⬅️", callback_data=f"view_links:{folder_id}")
    builder.adjust(1)
    
    await query.message.edit_text(
        message_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("all_link_stats:"))
async def all_link_stats(query: types.CallbackQuery):
    """Показать аналитику по всем ссылкам"""
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    links = db.get_all_links_for_folder(folder_id)
    
    if not links:
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data=f"view_links:{folder_id}")
        add_close_and_menu_buttons(builder)
        await query.message.edit_text(
            "📊 Нет созданных ссылок",
            reply_markup=builder.as_markup()
        )
        await query.answer()
        return

    message_text = f"📊 <b>Аналитика ссылок доступа</b>\n"
    message_text += f"📁 Папка: <b>{folder['name']}</b>\n\n"
    
    total_users = set()
    
    for link_id, token, name, permission, created_at in links:
        link_users = db.get_link_users(token)
        user_count = len(link_users)
        
        for tg_id, _ in link_users:
            total_users.add(tg_id)
        
        message_text += f"<b>🔗 {name}</b>\n"
        message_text += f"   🔐 {permission} • {user_count} перешли\n"
        
        if link_users:
            for tg_id, used_at in link_users[:3]:  # Показываем первых 3
                message_text += f"   👤 ID {tg_id} ({used_at})\n"
            if user_count > 3:
                message_text += f"   ... и ещё {user_count - 3}\n"
        message_text += "\n"
    
    message_text += f"\n📈 <b>Итого:</b> {len(total_users)} уникальных пользователей"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️", callback_data=f"view_links:{folder_id}")
    add_close_and_menu_buttons(builder)
    
    await query.message.edit_text(
        message_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("delete_link:"))
async def delete_link(query: types.CallbackQuery):
    """Удалить ссылку доступа"""
    parts = query.data.split(":")
    folder_id = int(parts[1])
    token = parts[2]
    
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    access_info = db.get_access_by_token(token)
    if not access_info:
        await query.answer("❌ Ссылка не найдена", show_alert=True)
        return

    db.delete_access_token(token)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=f"view_links:{folder_id}")
    add_close_and_menu_buttons(builder)
    
    await query.message.edit_text(
        "✅ Ссылка удалена",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("all_folder_users:"))
async def all_folder_users(query: types.CallbackQuery):
    """Показать всех пользователей которые получили доступ через ссылки"""
    folder_id = int(query.data.split(":")[1])
    user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if folder["owner_id"] != user_id:
        await query.answer("❌ Только владелец", show_alert=True)
        return

    link_users = db.get_all_link_users(folder_id)
    permissions = db.get_permissions(folder_id)
    
    if not link_users and not permissions:
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data=f"manage_access:{folder_id}")
        add_close_and_menu_buttons(builder)
        await query.message.edit_text(
            "👥 Никто ещё не получил доступ",
            reply_markup=builder.as_markup()
        )
        await query.answer()
        return

    message_text = f"👥 <b>Все пользователи папки '{folder['name']}'</b>\n\n"
    
    # Пользователи через ссылки
    if link_users:
        message_text += f"<b>Доступ через ссылки ({len(link_users)}):</b>\n"
        for tg_id, used_at in link_users:
            try:
                user_info = await bot.get_chat(tg_id)
                username = f" (@{user_info.username})" if user_info.username else ""
            except:
                username = ""
            
            message_text += f"👤 <a href='tg://user?id={tg_id}'>ID {tg_id}</a>{username}\n"
            message_text += f"   ⏰ {used_at}\n\n"
    
    # Пользователи с прямым доступом
    if permissions:
        message_text += f"<b>Прямой доступ ({len(permissions)}):</b>\n"
        for perm_id, tg_id, perm in permissions:
            try:
                user_info = await bot.get_chat(tg_id)
                username = f" (@{user_info.username})" if user_info.username else ""
            except:
                username = ""
            
            message_text += f"👤 <a href='tg://user?id={tg_id}'>ID {tg_id}</a>{username} ({perm})\n"
    
    builder = InlineKeyboardBuilder()
    
    builder.button(text="⬅️", callback_data=f"manage_access:{folder_id}")
    builder.adjust(1)
    add_close_and_menu_buttons(builder)
    
    await query.message.edit_text(
        message_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("add_user:"))
async def add_user_prompt(query: types.CallbackQuery, state: FSMContext):
    folder_id = int(query.data.split(":")[1])
    await state.update_data(folder_id=folder_id)
    await state.set_state(States.adding_user)
    await query.message.edit_text("👤 Введите Telegram ID пользователя:")
    await query.answer()

@dp.message(States.adding_user)
async def add_user_handler(message: types.Message, state: FSMContext):
    try:
        target_tg_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введите корректный ID")
        return

    data = await state.get_data()
    folder_id = data["folder_id"]

    await state.update_data(target_tg_id=target_tg_id)
    await state.set_state(States.giving_permission)
    await message.answer("📝 Выберите право:\n1️⃣ read - просмотр\n2️⃣ write - загрузка/удаление")

@dp.message(States.giving_permission)
async def give_permission_handler(message: types.Message, state: FSMContext):
    if message.text == "1":
        perm = "read"
    elif message.text == "2":
        perm = "write"
    else:
        await message.answer("❌ Выберите 1 или 2")
        return

    data = await state.get_data()
    folder_id = data["folder_id"]
    target_tg_id = data["target_tg_id"]

    folder = db.get_folder(folder_id)
    request_id = db.create_access_request(folder_id, target_tg_id, perm)

    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять", callback_data=f"accept_access:{request_id}")
    builder.button(text="❌ Отказать", callback_data=f"reject_access:{request_id}")
    builder.adjust(2)

    try:
        await bot.send_message(
            target_tg_id,
            f"📨 Вам предоставлен доступ к папке:\n\n📁 <b>{folder['name']}</b>\n\n"
            f"Тип доступа: <b>{'Просмотр' if perm == 'read' else 'Загрузка/Удаление'}</b>\n\n"
            f"Примете доступ?",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except:
        await message.answer(f"⚠️ Не смог отправить сообщение пользователю {target_tg_id}")

    user_id = db.get_or_create_user(message.from_user.id)
    await message.answer(
        "✅ Запрос отправлен!",
        reply_markup=build_folder_keyboard(folder_id, user_id)
    )
    await state.clear()

@dp.callback_query(F.data.startswith("accept_access:"))
async def accept_access_callback(query: types.CallbackQuery):
    request_id = int(query.data.split(":")[1])
    req = db.get_access_request(request_id)

    if not req:
        await query.answer("❌ Запрос не найден", show_alert=True)
        return

    folder = db.get_folder(req["folder_id"])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, уведомлять", callback_data=f"accept_with_notify:{request_id}:1")
    builder.button(text="❌ Нет, не нужно", callback_data=f"accept_with_notify:{request_id}:0")
    builder.adjust(1)

    await query.message.edit_text(
        f"📋 Вы получили доступ к папке '{folder['name']}'!\n\n"
        f"Тип доступа: {req['permission']}\n\n"
        f"Получать уведомления об изменениях в этой папке?",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("reject_access:"))
async def reject_access_callback(query: types.CallbackQuery):
    request_id = int(query.data.split(":")[1])
    req = db.get_access_request(request_id)

    if not req:
        await query.answer("❌ Запрос не найден", show_alert=True)
        return

    db.reject_access(request_id)
    folder = db.get_folder(req["folder_id"])

    await query.message.edit_text(
        f"❌ Вы отклонили доступ к папке:\n\n📁 <b>{folder['name']}</b>",
        parse_mode="HTML"
    )
    await query.answer()



@dp.callback_query(F.data.startswith("accept_with_notify:"))
async def accept_with_notify(query: types.CallbackQuery):
    """Подтвердить доступ с выбором уведомлений"""
    parts = query.data.split(":")
    request_id = int(parts[1])
    notify = bool(int(parts[2]))
    
    req = db.get_access_request(request_id)
    if not req:
        await query.answer("❌ Запрос не найден", show_alert=True)
        return
    
    db.accept_access(request_id, notify_on_changes=notify)
    folder = db.get_folder(req["folder_id"])

    builder = InlineKeyboardBuilder()
    builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{req['folder_id']}")
    builder.button(text="🏠 В главное меню", callback_data="go_home")
    builder.adjust(1)

    notify_text = "✅ Вы будете получать уведомления об изменениях" if notify else "ℹ️ Уведомления отключены"
    
    await query.message.edit_text(
        f"✅ Вы приняли доступ к папке:\n\n📁 <b>{folder['name']}</b>\n\n{notify_text}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("confirm_access_from_link:"))
async def confirm_access_from_link(query: types.CallbackQuery):
    """Подтвердить доступ из ссылки с выбором уведомлений"""
    parts = query.data.split(":")
    folder_id = int(parts[1])
    permission = parts[2]
    
    folder = db.get_folder(folder_id)
    db_user_id = db.get_or_create_user(query.from_user.id)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, уведомлять", callback_data=f"save_link_access:{folder_id}:1")
    builder.button(text="❌ Нет, не нужно", callback_data=f"save_link_access:{folder_id}:0")
    builder.adjust(1)

    await query.message.edit_text(
        f"📋 Получать уведомления об изменениях в папке '{folder['name']}'?",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("save_link_access:"))
async def save_link_access(query: types.CallbackQuery):
    """Сохранить предпочтение уведомлений для доступа по ссылке"""
    parts = query.data.split(":")
    folder_id = int(parts[1])
    notify = bool(int(parts[2]))
    
    folder = db.get_folder(folder_id)
    db_user_id = db.get_or_create_user(query.from_user.id)
    
    # Обновить настройку уведомлений
    db.cursor.execute(
        "UPDATE folder_permissions SET notify_on_changes = ? WHERE folder_id = ? AND user_id = ?",
        (1 if notify else 0, folder_id, db_user_id)
    )
    db.conn.commit()

    builder = InlineKeyboardBuilder()
    builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{folder_id}")
    builder.button(text="🏠 В главное меню", callback_data="go_home")
    builder.adjust(1)

    notify_text = "✅ Вы будете получать уведомления об изменениях" if notify else "ℹ️ Уведомления отключены"
    
    await query.message.edit_text(
        f"✅ Доступ подтвержден!\n\n📁 {folder['name']}\n\n{notify_text}",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data == "go_home")
async def go_home(query: types.CallbackQuery):
    """Вернуться в главное меню"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📁 Мое хранилище", callback_data="my_storage")
    builder.button(text="🔗 Доступные папки", callback_data="shared_folders")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="close_message"))
    
    await query.message.edit_text(
        "🌟 <b>ОБЛАЧНОЕ ХРАНИЛИЩЕ</b> 🌟\n\n"
        "👋 <b>Вы в главном меню</b>\n\n"
        "Выберите действие:\n"
        "• 📁 <b>Мое хранилище</b> - управление вашими файлами\n"
        "• 🔗 <b>Доступные папки</b> - папки, к которым вам дан доступ",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()


@dp.callback_query(F.data.startswith("move_file_up:"))
async def move_file_up(query: types.CallbackQuery):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file_write(db_user_id, file_id):
        await query.answer("❌ Нет прав", show_alert=True)
        return

    if db.move_file_up_hierarchy(file_id, db_user_id):
        current_folder = db.get_folder(file["folder_id"])
        await query.message.edit_text(
            f"✅ Файл перемещён!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(text="⬅️ Назад", callback_data=f"open_folder:{current_folder['parent_id']}")
                ]]
            )
        )
    else:
        await query.answer("❌ Невозможно переместить выше", show_alert=True)
    
    await query.answer()

@dp.callback_query(F.data.startswith("move_file_down:"))
async def move_file_down(query: types.CallbackQuery):
    file_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    file = db.get_file(file_id)

    if not file or not db.can_access_file_write(db_user_id, file_id):
        await query.answer("❌ Нет прав", show_alert=True)
        return

    # Показать список подпапок для перемещения
    subfolders = db.get_contents(file["folder_id"])[0]  # Только папки
    
    if not subfolders:
        await query.answer("❌ Нет подпапок для перемещения", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for subfolder_id, subfolder_name in subfolders:
        builder.button(text=f"📁 {subfolder_name}", callback_data=f"move_file_to:{file_id}:{subfolder_id}")
    
    builder.button(text="❌ Отмена", callback_data=f"file_menu:{file_id}")
    builder.adjust(1)
    
    await query.message.edit_text(
        "📁 Выберите папку для перемещения файла:",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("move_file_to:"))
async def move_file_to_folder(query: types.CallbackQuery):
    parts = query.data.split(":")
    file_id = int(parts[1])
    subfolder_id = int(parts[2])
    db_user_id = db.get_or_create_user(query.from_user.id)
    
    file = db.get_file(file_id)
    if not file or not db.check_access(db_user_id, file["folder_id"], "write"):
        await query.answer("❌ Нет прав", show_alert=True)
        return
    
    if db.move_file_to_subfolder(file_id, subfolder_id, db_user_id):
        folder_path = db.get_folder_path(subfolder_id)
        await query.message.edit_text(
            f"✅ Файл перемещён!\n📁 {folder_path}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(text="⬅️ Назад", callback_data=f"open_folder:{subfolder_id}")
                ]]
            )
        )
    else:
        await query.answer("❌ Ошибка при перемещении", show_alert=True)
    
    await query.answer()

@dp.callback_query(F.data.startswith("delete_folder:"))
async def delete_folder_confirm(query: types.CallbackQuery):
    folder_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if not folder:
        await query.answer("❌ Папка не найдена", show_alert=True)
        return

    # Проверить права на удаление
    is_root = folder["parent_id"] is None
    can_delete = folder["owner_id"] == db_user_id and not is_root
    
    if not can_delete:
        await query.answer("❌ Нет прав на удаление", show_alert=True)
        return

    # Показать подтверждение
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"confirm_delete_folder:{folder_id}")
    builder.button(text="❌ Отмена", callback_data=f"open_folder:{folder_id}")
    builder.adjust(1)

    await query.message.edit_text(
        f"⚠️ Вы уверены что хотите удалить папку '{folder['name']}'?\n\n"
        f"Будут удалены все файлы и подпапки внутри!",
        reply_markup=builder.as_markup()
    )
    await query.answer()

@dp.callback_query(F.data.startswith("confirm_delete_folder:"))
async def confirm_delete_folder(query: types.CallbackQuery):
    folder_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)

    if not folder:
        await query.answer("❌ Папка не найдена", show_alert=True)
        return

    # Проверить права ещё раз
    is_root = folder["parent_id"] is None
    can_delete = folder["owner_id"] == db_user_id and not is_root
    
    if not can_delete:
        await query.answer("❌ Нет прав на удаление", show_alert=True)
        return

    # Удалить папку
    parent_id = folder["parent_id"]
    folder_name = folder["name"]
    parent_path_str = db.get_folder_path(parent_id) if parent_id else "Корень"
    
    # Отправить уведомления пользователям родительской папки перед удалением
    if parent_id:
        users_to_notify = db.get_users_to_notify(parent_id)
        for tg_id in users_to_notify:
            try:
                builder = InlineKeyboardBuilder()
                builder.button(text="📁 Открыть папку", callback_data=f"open_folder:{parent_id}")
                builder.button(text="🔔 Уведомления", callback_data=f"toggle_notify:{parent_id}")
                builder.adjust(1)
                
                await bot.send_message(
                    tg_id,
                    f"📁 Папка: <b>{parent_path_str}</b>\n\n"
                    f"🗑️ Удалена папка: <b>{folder_name}</b>",
                    parse_mode="HTML",
                    reply_markup=builder.as_markup()
                )
            except:
                pass  # Игнорируем ошибки отправки
    
    if db.delete_folder_recursive(folder_id):
        await query.message.edit_text(
            "✅ Папка удалена!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="⬅️ Назад",
                        callback_data=f"open_folder:{parent_id}" if parent_id else "go_home"
                    )
                ]]
            )
        )
    else:
        await query.answer("❌ Ошибка при удалении", show_alert=True)
    
    await query.answer()

@dp.callback_query(F.data.startswith("toggle_notify:"))
async def toggle_notify(query: types.CallbackQuery):
    """Переключить статус уведомлений для папки"""
    folder_id = int(query.data.split(":")[1])
    db_user_id = db.get_or_create_user(query.from_user.id)
    folder = db.get_folder(folder_id)
    
    if not folder or not db.check_access(db_user_id, folder_id):
        await query.answer("❌ Нет доступа", show_alert=True)
        return
    
    if folder["owner_id"] == db_user_id:
        await query.answer("❌ Это ваша папка", show_alert=True)
        return
    
    # Переключить уведомления
    new_status = db.toggle_notification(folder_id, db_user_id)
    notify_text = "✅ Уведомления включены" if new_status else "ℹ️ Уведомления отключены"
    
    await query.answer(notify_text, show_alert=False)
    
    # Обновить меню папки
    tree = db.get_full_tree(folder_id)
    message_text = f"📁 {db.get_folder_path(folder_id)}"
    if tree:
        message_text += f"\n\n<code>{tree}</code>"
    
    await query.message.edit_text(
        message_text,
        reply_markup=build_folder_keyboard(folder_id, db_user_id),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "close_message")
async def close_message(query: types.CallbackQuery):
    """Удалить сообщение"""
    try:
        await query.message.delete()
        await query.answer()
    except Exception as e:
        await query.answer("❌ Не удалось удалить сообщение", show_alert=True)

@dp.message(Command("stats"))
async def stats_command(message: types.Message):
    """Показать статистику использования"""
    db_user_id = db.get_or_create_user(message.from_user.id)
    
    tariff = db.get_user_tariff(message.from_user.id)
    usage = db.get_user_usage(db_user_id)
    
    message_text = f"""
📊 <b>СТАТИСТИКА</b>

<b>Текущий тариф:</b> {tariff['name']}
💰 <b>Цена:</b> {'Бесплатно' if tariff['price_stars'] == 0 else f"{tariff['price_stars']}⭐/месяц"}

📈 <b>ИСПОЛЬЗОВАНИЕ:</b>
• 📁 Папок: {usage['folder_count']}/{tariff['folder_count_limit']}
• 📄 Файлов: {usage['file_count']}/{tariff['file_count_limit']}

⭐ <b>ДОСТУПНЫЕ ТАРИФЫ:</b>
"""
    
    keyboard = InlineKeyboardBuilder()
    for t in db.get_all_tariffs():
        if t['price_stars'] == 0:
            keyboard.button(text=f"✓ {t['name']} (Бесплатно)", callback_data="noop")
        else:
            keyboard.button(text=f"⭐ {t['name']} ({t['price_stars']}★)", callback_data=f"buy_tariff_{t['id']}")
    
    keyboard.button(text="❌ Закрыть", callback_data="close_message")
    keyboard.adjust(1)
    
    await message.answer(message_text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("buy_tariff_"))
async def buy_tariff(query: types.CallbackQuery):
    """Показать счёт на оплату для тарифа или применить бесплатный"""
    try:
        tariff_id = int(query.data.split("_")[2])
        tariff = None
        for t in db.get_all_tariffs():
            if t['id'] == tariff_id:
                tariff = t
                break
        
        if not tariff:
            await query.answer("❌ Этот тариф недоступен", show_alert=True)
            return
        
        # Если бесплатный тариф - просто применить
        if tariff['price_stars'] == 0:
            user_id = db.get_or_create_user(query.from_user.id)
            if db.assign_tariff_to_user(user_id, tariff_id):
                await query.answer("✅ Бесплатный тариф активирован", show_alert=True)
            else:
                await query.answer("❌ Ошибка при активации", show_alert=True)
            return
        
        prices = [LabeledPrice(label=f"⭐ {tariff['price_stars']} звёзд - 30 дней", amount=tariff['price_stars'])]
        
        await bot.send_invoice(
            chat_id=query.from_user.id,
            title=f"⭐ {tariff['price_stars']} звёзд - {tariff['name']}",
            description=f"📁 {tariff['folder_count_limit']} папок, 📄 {tariff['file_count_limit']} файлов",
            payload=f"tariff_{tariff_id}",
            provider_token="",
            currency="XTR",
            prices=prices
        )
        await query.answer()
    except Exception as e:
        print(f"Error in buy_tariff: {e}")
        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)

@dp.pre_checkout_query()
async def pre_checkout(pre_checkout: types.PreCheckoutQuery):
    """Проверка перед оплатой"""
    await bot.answer_pre_checkout_query(pre_checkout.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    """Успешная оплата"""
    try:
        payload = message.successful_payment.invoice_payload
        if payload.startswith("tariff_"):
            tariff_id = int(payload.split("_")[1])
            db.subscribe_user_to_tariff(message.from_user.id, tariff_id)
            
            tariff = None
            for t in db.get_all_tariffs():
                if t['id'] == tariff_id:
                    tariff = t
                    break
            
            await message.answer(
                f"✅ Спасибо за покупку!\n\n"
                f"Вы подписались на тариф <b>{tariff['name']}</b> на 30 дней.\n\n"
                f"📊 <b>Ваши лимиты:</b>\n"
                f"• 📁 Папки: {tariff['folder_count_limit']}\n"
                f"• 📄 Файлы: {tariff['file_count_limit']}\n"
                f"• 💾 Память: {tariff['storage_limit']}GB",
                parse_mode="HTML"
            )
    except Exception as e:
        print(f"Error in successful_payment: {e}")
        await message.answer("❌ Ошибка при обработке платежа")

# ============== ADMIN PANEL ==============

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    """Открыть админ панель (только для админов)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа")
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Управление тарифами", callback_data="admin_tariffs")
    builder.button(text="👥 Управление пользователями", callback_data="admin_users")
    builder.button(text="📈 Статистика", callback_data="admin_stats")
    builder.adjust(1)
    
    await message.answer(
        "⚙️ <b>АДМИН ПАНЕЛЬ</b> ⚙️\n\n"
        "Выберите действие:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_tariffs")
async def admin_tariffs(query: types.CallbackQuery):
    """Управление тарифами"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    tariffs = db.get_all_tariffs()
    builder = InlineKeyboardBuilder()
    
    builder.button(text="➕ Создать новый тариф", callback_data="admin_create_tariff")
    
    if tariffs:
        for tariff in tariffs:
            builder.button(
                text=f"📌 {tariff['name']} ({tariff['price_stars']}⭐)",
                callback_data=f"admin_tariff_details:{tariff['id']}"
            )
    
    builder.button(text="⬅️ Назад", callback_data="admin_back_to_menu")
    builder.adjust(1)
    
    message_text = "📊 <b>УПРАВЛЕНИЕ ТАРИФАМИ</b>\n\n"
    if tariffs:
        message_text += "<b>Доступные тарифы:</b>"
    else:
        message_text += "<i>Нет созданных тарифов</i>"
    
    await query.message.edit_text(message_text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await query.answer()

@dp.callback_query(F.data == "admin_create_tariff")
async def admin_create_tariff(query: types.CallbackQuery, state: FSMContext):
    """Создать новый тариф"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    await state.set_state(States.admin_tariff_name)
    await query.message.edit_text("📝 Введите название тарифа (например: Премиум):")
    await query.answer()

@dp.message(States.admin_tariff_name)
async def admin_tariff_name_input(message: types.Message, state: FSMContext):
    """Ввод названия тарифа"""
    await state.update_data(tariff_name=message.text.strip())
    await state.set_state(States.admin_tariff_files)
    await message.answer("📄 Введите лимит файлов (например: 100):")

@dp.message(States.admin_tariff_files)
async def admin_tariff_files_input(message: types.Message, state: FSMContext):
    """Ввод лимита файлов"""
    try:
        files = int(message.text)
        await state.update_data(file_count_limit=files)
        await state.set_state(States.admin_tariff_folders)
        await message.answer("📁 Введите лимит папок (например: 50):")
    except ValueError:
        await message.answer("❌ Введите число")

@dp.message(States.admin_tariff_folders)
async def admin_tariff_folders_input(message: types.Message, state: FSMContext):
    """Ввод лимита папок"""
    try:
        folders = int(message.text)
        await state.update_data(folder_count_limit=folders)
        await state.set_state(States.admin_tariff_price)
        await message.answer("⭐ Введите цену в звездах (0 для бесплатного):")
    except ValueError:
        await message.answer("❌ Введите число")

@dp.message(States.admin_tariff_price)
async def admin_tariff_price_input(message: types.Message, state: FSMContext):
    """Ввод цены тарифа"""
    try:
        price = int(message.text)
        data = await state.get_data()
        
        # Создать тариф
        db.cursor.execute(
            "INSERT INTO tariffs (name, file_count_limit, folder_count_limit, price_stars, description) VALUES (?, ?, ?, ?, ?)",
            (data['tariff_name'], data['file_count_limit'], data['folder_count_limit'], price, f"📁 {data['folder_count_limit']} папок, 📄 {data['file_count_limit']} файлов")
        )
        db.conn.commit()
        
        builder = InlineKeyboardBuilder()
        builder.button(text="📊 К тарифам", callback_data="admin_tariffs")
        
        await message.answer(
            f"✅ Тариф успешно создан!\n\n"
            f"📌 Имя: {data['tariff_name']}\n"
            f"📄 Файлы: {data['file_count_limit']}\n"
            f"📁 Папки: {data['folder_count_limit']}\n"
            f"⭐ Цена: {price}⭐",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число")

@dp.callback_query(F.data.startswith("admin_tariff_details:"))
async def admin_tariff_details(query: types.CallbackQuery):
    """Детали тарифа"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    tariff_id = int(query.data.split(":")[1])
    tariff = next((t for t in db.get_all_tariffs() if t['id'] == tariff_id), None)
    
    if not tariff:
        await query.answer("❌ Тариф не найден", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data=f"admin_edit_tariff:{tariff_id}")
    builder.button(text="🗑️ Удалить", callback_data=f"admin_delete_tariff:{tariff_id}")
    builder.button(text="⬅️ Назад", callback_data="admin_tariffs")
    builder.adjust(1)
    
    message_text = (
        f"📌 <b>{tariff['name']}</b>\n\n"
        f" Файлы: {tariff['file_count_limit']}\n"
        f"📁 Папки: {tariff['folder_count_limit']}\n"
        f"⭐ Цена: {tariff['price_stars']}⭐"
    )
    
    await query.message.edit_text(message_text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await query.answer()

@dp.callback_query(F.data.startswith("admin_delete_tariff:"))
async def admin_delete_tariff(query: types.CallbackQuery):
    """Удалить тариф"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    tariff_id = int(query.data.split(":")[1])
    
    if db.delete_tariff(tariff_id):
        await query.answer("✅ Тариф удален", show_alert=True)
    else:
        await query.answer("❌ Ошибка при удалении", show_alert=True)
    
    # Вернуться к списку тарифов
    await admin_tariffs(query)

@dp.callback_query(F.data == "admin_users")
async def admin_users(query: types.CallbackQuery):
    """Управление пользователями"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Найти пользователя", callback_data="admin_find_user")
    builder.button(text="⬅️ Назад", callback_data="admin_back_to_menu")
    builder.adjust(1)
    
    await query.message.edit_text(
        "👥 <b>УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ</b>\n\n"
        "Выберите действие:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data == "admin_find_user")
async def admin_find_user_prompt(query: types.CallbackQuery, state: FSMContext):
    """Поиск пользователя"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    await state.set_state(States.admin_find_user)
    await query.message.edit_text("🔍 Введите Telegram ID пользователя:")
    await query.answer()

@dp.message(States.admin_find_user)
async def admin_find_user_handler(message: types.Message, state: FSMContext):
    """Обработка поиска пользователя"""
    try:
        tg_id = int(message.text)
        user = db.find_user_by_tg_id(tg_id)
        
        if not user:
            await message.answer("❌ Пользователь не найден")
            return
        
        user_id, _ = user
        tariff = db.get_user_tariff(tg_id)
        usage = db.get_user_usage(user_id)
        
        builder = InlineKeyboardBuilder()
        builder.button(text="⭐ Назначить тариф", callback_data=f"admin_assign_tariff:{user_id}")
        builder.adjust(1)
        
        message_text = (
            f"👤 <b>Пользователь: {tg_id}</b>\n\n"
            f"📊 <b>Текущий тариф:</b> {tariff['name']}\n"
            f"⭐ <b>Цена:</b> {tariff['price_stars']}⭐/месяц\n\n"
            f"📈 <b>Использование:</b>\n"
            f"• 📁 Папок: {usage['folder_count']}/{tariff['folder_count_limit']}\n"
            f"• 📄 Файлов: {usage['file_count']}/{tariff['file_count_limit']}"
        )
        
        await state.update_data(user_id=user_id, tg_id=tg_id)
        await message.answer(message_text, reply_markup=builder.as_markup(), parse_mode="HTML")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите корректный Telegram ID")

@dp.callback_query(F.data.startswith("admin_assign_tariff:"))
async def admin_assign_tariff_menu(query: types.CallbackQuery):
    """Меню выбора тарифа для пользователя"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    user_id = int(query.data.split(":")[1])
    tariffs = db.get_all_tariffs()
    
    builder = InlineKeyboardBuilder()
    for tariff in tariffs:
        builder.button(
            text=f"{tariff['name']} ({tariff['price_stars']}⭐)",
            callback_data=f"admin_confirm_tariff:{user_id}:{tariff['id']}"
        )
    
    builder.adjust(1)
    
    await query.message.edit_text(
        "⭐ <b>Выберите тариф для назначения:</b>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("admin_confirm_tariff:"))
async def admin_confirm_tariff(query: types.CallbackQuery):
    """Подтверждение назначения тарифа"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    parts = query.data.split(":")
    user_id = int(parts[1])
    tariff_id = int(parts[2])
    
    if db.assign_tariff_to_user(user_id, tariff_id):
        tariff = next((t for t in db.get_all_tariffs() if t['id'] == tariff_id), None)
        await query.answer(f"✅ Тариф '{tariff['name']}' назначен", show_alert=True)
    else:
        await query.answer("❌ Ошибка при назначении", show_alert=True)
    
    await admin_users(query)

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(query: types.CallbackQuery):
    """Общая статистика"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    db.cursor.execute("SELECT COUNT(*) FROM users")
    total_users = db.cursor.fetchone()[0]
    
    db.cursor.execute("SELECT COUNT(*) FROM folders")
    total_folders = db.cursor.fetchone()[0]
    
    db.cursor.execute("SELECT COUNT(*) FROM files")
    total_files = db.cursor.fetchone()[0]
    
    db.cursor.execute("SELECT COUNT(*) FROM tariffs")
    total_tariffs = db.cursor.fetchone()[0]
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="admin_back_to_menu")
    
    await query.message.edit_text(
        f"📈 <b>СТАТИСТИКА СИСТЕМЫ</b>\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"📁 Папок: {total_folders}\n"
        f"📄 Файлов: {total_files}\n"
        f"⭐ Тарифов: {total_tariffs}",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

@dp.callback_query(F.data.startswith("admin_edit_tariff:"))
async def admin_edit_tariff(query: types.CallbackQuery, state: FSMContext):
    """Редактировать тариф"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    tariff_id = int(query.data.split(":")[1])
    tariff = next((t for t in db.get_all_tariffs() if t['id'] == tariff_id), None)
    
    if not tariff:
        await query.answer("❌ Тариф не найден", show_alert=True)
        return
    
    await state.update_data(edit_tariff_id=tariff_id, tariff=tariff)
    await state.set_state(States.admin_edit_tariff_name)
    await query.message.edit_text(f"📝 Введите новое название тарифа:\n\nТекущее: {tariff['name']}")
    await query.answer()

@dp.message(States.admin_edit_tariff_name)
async def admin_edit_tariff_name_input(message: types.Message, state: FSMContext):
    """Ввод названия при редактировании"""
    data = await state.get_data()
    await state.update_data(new_name=message.text.strip())
    await state.set_state(States.admin_edit_tariff_files)
    await message.answer(f"📄 Введите новый лимит файлов:\n\nТекущий: {data['tariff']['file_count_limit']}")

@dp.message(States.admin_edit_tariff_files)
async def admin_edit_tariff_files_input(message: types.Message, state: FSMContext):
    """Ввод файлов при редактировании"""
    try:
        files = int(message.text)
        data = await state.get_data()
        await state.update_data(new_files=files)
        await state.set_state(States.admin_edit_tariff_folders)
        await message.answer(f"📁 Введите новый лимит папок:\n\nТекущий: {data['tariff']['folder_count_limit']}")
    except ValueError:
        await message.answer("❌ Введите число")

@dp.message(States.admin_edit_tariff_folders)
async def admin_edit_tariff_folders_input(message: types.Message, state: FSMContext):
    """Ввод папок при редактировании"""
    try:
        folders = int(message.text)
        data = await state.get_data()
        await state.update_data(new_folders=folders)
        await state.set_state(States.admin_edit_tariff_price)
        await message.answer(f"⭐ Введите новую цену в звездах:\n\nТекущая: {data['tariff']['price_stars']}")
    except ValueError:
        await message.answer("❌ Введите число")

@dp.message(States.admin_edit_tariff_price)
async def admin_edit_tariff_price_input(message: types.Message, state: FSMContext):
    """Ввод цены при редактировании"""
    try:
        price = int(message.text)
        data = await state.get_data()
        
        # Обновить тариф
        if db.update_tariff(
            data['edit_tariff_id'],
            data['new_name'],
            data['new_files'],
            data['new_folders'],
            price
        ):
            builder = InlineKeyboardBuilder()
            builder.button(text="📊 К тарифам", callback_data="admin_tariffs")
            
            await message.answer(
                f"✅ Тариф успешно обновлен!\n\n"
                f"📌 Имя: {data['new_name']}\n"
                f"📄 Файлы: {data['new_files']}\n"
                f"📁 Папки: {data['new_folders']}\n"
                f"⭐ Цена: {price}⭐",
                reply_markup=builder.as_markup(),
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ Ошибка при обновлении тарифа")
        
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число")

@dp.callback_query(F.data == "admin_back_to_menu")
async def admin_back_to_menu(query: types.CallbackQuery):
    """Вернуться в админ меню"""
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("❌ Доступ запрещен", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Управление тарифами", callback_data="admin_tariffs")
    builder.button(text="👥 Управление пользователями", callback_data="admin_users")
    builder.button(text="📈 Статистика", callback_data="admin_stats")
    builder.adjust(1)
    
    await query.message.edit_text(
        "⚙️ <b>АДМИН ПАНЕЛЬ</b> ⚙️\n\n"
        "Выберите действие:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await query.answer()

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
