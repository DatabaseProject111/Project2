import sqlite3
import psycopg2

# 连接到 SQLite 数据库
sqlite_conn = sqlite3.connect('E:\\E盘内容\\课件\\数据管理系统\\book_lx.db')
sqlite_cursor = sqlite_conn.cursor()

# 连接到 PostgreSQL 数据库
postgres_conn = psycopg2.connect(
    dbname='bookstore',
    user='postgres',
    password='zzh0117.',
    host='127.0.0.1',
    port='5432'
)
postgres_cursor = postgres_conn.cursor()

# 创建book表

book_create = """
     CREATE TABLE IF NOT EXISTS "book" (
        id TEXT PRIMARY KEY,
        title TEXT,
        author TEXT,
        publisher TEXT,
        original_title TEXT,
        translator TEXT,
        pub_year TEXT,
        pages INTEGER,
        price INTEGER,
        currency_unit TEXT,
        binding TEXT,
        isbn TEXT,
        author_intro TEXT,
        book_intro TEXT,
        content TEXT,
        tags TEXT,
        picture BYTEA
                );
"""

postgres_cursor.execute(book_create)

# 检索数据并将其插入到 PostgreSQL 数据库中
sqlite_cursor.execute("SELECT * FROM book")
rows = sqlite_cursor.fetchall()

for row in rows:
    # 排除不需要导入的列
    id_, title, author, publisher, original_title, translator, pub_year, pages, price, currency_unit, binding, isbn, _, _, _, tags, _ = row

    # 构建插入语句
    insert_query = "INSERT INTO book (id, title, author, publisher, original_title, translator, pub_year, pages, price, currency_unit, binding, isbn, tags) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # 执行插入操作
    postgres_cursor.execute(insert_query, (id_, title, author, publisher, original_title, translator, pub_year, pages, price, currency_unit, binding, isbn, tags))

# 提交事务并关闭连接
postgres_conn.commit()
postgres_conn.close()
sqlite_conn.close()

print("数据已成功导入到 PostgreSQL 数据库中。")
