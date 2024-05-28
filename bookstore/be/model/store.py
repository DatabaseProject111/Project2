import logging
import os
# import sqlite3 as sqlite
# import threading
import psycopg2
import threading
from psycopg2 import sql


class Store:
    database: str

    def __init__(self, db_path):
        # self.database = os.path.join(db_path, "be.db")
        self.database = db_path
        self.init_tables()

    def init_tables(self):
        conn = None  # 初始化 conn 变量
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS "user" (
                    user_id TEXT PRIMARY KEY, 
                    password TEXT NOT NULL, 
                    balance INTEGER NOT NULL, 
                    token TEXT, 
                    terminal TEXT
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS "user_store" (
                    user_id TEXT, 
                    store_id TEXT, 
                    PRIMARY KEY (user_id, store_id),
                    FOREIGN KEY (user_id) REFERENCES "user" (user_id)
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS "store" (
                    store_id TEXT, 
                    book_id TEXT, 
                    book_info TEXT, 
                    stock_level INTEGER,
                    PRIMARY KEY (store_id, book_id)
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS "new_order" (
                    order_id TEXT PRIMARY KEY, 
                    user_id TEXT, 
                    store_id TEXT
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS "new_order_detail" (
                    order_id TEXT, 
                    book_id TEXT, 
                    count INTEGER, 
                    price INTEGER,  
                    PRIMARY KEY (order_id, book_id)
                );
                """
            )
            conn.commit()
            cursor.close()
            # conn.close()
        # except sqlite.Error as e:
        #     logging.error(e)
        #     conn.rollback()
        except psycopg2.Error as e:
            logging.error(e)
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    # def get_db_conn(self) -> sqlite.Connection:
    #     return sqlite.connect(self.database)
    def get_db_conn(self):
        # 使用 PostgreSQL 连接
        try:
            conn = psycopg2.connect(self.database)
            return conn
        except psycopg2.Error as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            return None


database_instance: Store = None
# global variable for database sync
init_completed_event = threading.Event()


def init_database(db_path):
    global database_instance
    database_instance = Store(db_path)
    database_instance.init_tables()


def get_db_conn():
    global database_instance
    return database_instance.get_db_conn()
