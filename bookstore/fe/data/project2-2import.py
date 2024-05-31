import pymongo


# 连接到 MongoDB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client['bookstore']
mongo_collection = mongo_db['book']


# 查询 MongoDB 中的数据，只包括需要的字段
mongo_data = mongo_collection.find({}, {'author_intro': 1, 'book_intro': 1, 'content': 1, 'picture': 1})

# 在这里处理查询到的数据a
for data in mongo_data:
    author_intro = data.get('author_intro', '')
    book_intro = data.get('book_intro', '')
    picture = data.get('picture', '')

    # 在这里进行进一步的处理，比如打印数据或者传递给其他函数
    # user.py
    def process_mongo_data(data):
        # 在这里进行进一步的处理，比如存储到数据库或进行业务逻辑处理
        # 例如：
        # User.objects.create(author_intro=data['author_intro'], ...)
        pass

# 提交事务并关闭连接
mongo_client.close()

print("数据已成功从 MongoDB 查询。")
