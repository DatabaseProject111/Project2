# import sqlite3 as sqlite
import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import psycopg2
from datetime import datetime, timedelta
import pytz


class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(
        self, user_id: str, store_id: str, id_and_count: [(str, int)]
    ) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))
            

            for book_id, count in id_and_count:
                #新增游标
                cursor = self.conn.cursor()
                query = "SELECT book_id, stock_level, book_info FROM \"store\" WHERE store_id = %s AND book_id = %s;"
                cursor.execute(query,(store_id, book_id))
                row = cursor.fetchone()
                cursor.close()
                if row is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = row[1]
                book_info = row[2]
                book_info_json = json.loads(book_info)
                price = book_info_json.get("price")

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE \"store\" set stock_level = stock_level - %s "
                    "WHERE store_id = %s and book_id = %s and stock_level >= %s; ",
                    (count, store_id, book_id, count),
                )
                if cursor.rowcount == 0:
                    cursor.close()
                    return error.error_stock_level_low(book_id) + (order_id,)
                cursor.execute(
                    "INSERT INTO \"new_order_detail\"(order_id, book_id, count, price) "
                    "VALUES(%s, %s, %s, %s);",
                    (uid, book_id, count, price),
                )
                cursor.close()

            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO \"new_order\"(order_id, store_id, user_id) "
                "VALUES(%s, %s, %s);",
                (uid, store_id, user_id),
            )
            self.conn.commit()
            cursor.close()
            order_id = uid
            # order_col = self.db["new_order"]
            # # 添加订单状态-代付款
            # # 默认订单状态为待付款
            # order_status = "unpaid"
            # order_col.insert_one({
            #     "order_id": uid,
            #     "store_id": store_id,
            #     "user_id": user_id,
            #     "status": order_status,  # 添加订单状态
            #     "order_date": datetime.now(),  # 添加订单创建时间
            #     # "expire_time": datetime.now(pytz.timezone.utc) + timedelta(minutes=30)  # 添加订单超时时间
            # })
            # order_id = uid
            
        except psycopg2.Error as e:
            logging.info("528, {}".format(str(e)))
            return 528, "{}".format(str(e)), ""
        # except BaseException as e:
        #     logging.info("530, {}".format(str(e)))
        #     return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        conn = self.conn
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT order_id, user_id, store_id FROM \"new_order\" WHERE order_id = %s",
                (order_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return error.error_invalid_order_id(order_id)

            order_id = row[0]
            buyer_id = row[1]
            store_id = row[2]

            if buyer_id != user_id:
                return error.error_authorization_fail()
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT balance, password FROM \"user\" WHERE user_id = %s;", (buyer_id,)
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return error.error_non_exist_user_id(buyer_id)
            balance = row[0]
            if password != row[1]:
                return error.error_authorization_fail()
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT store_id, user_id FROM \"user_store\" WHERE store_id = %s;",
                (store_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return error.error_non_exist_store_id(store_id)

            seller_id = row[1]

            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT book_id, count, price FROM \"new_order_detail\" WHERE order_id = %s;",
                (order_id,),
            )
            total_price = 0
            for row in cursor:
                count = row[1]
                price = row[2]
                total_price = total_price + price * count

            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)
            cursor.close()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE \"user\" SET balance = balance - %s WHERE user_id = %s AND balance >= %s",
                (total_price, buyer_id, total_price),
            )
            if cursor.rowcount == 0:
                cursor.close()
                return error.error_not_sufficient_funds(order_id)

            cursor.execute(
                "UPDATE \"user\" SET balance = balance + %s WHERE user_id = %s",
                (total_price, seller_id),
            )

            if cursor.rowcount == 0:
                cursor.close()
                return error.error_non_exist_user_id(seller_id)

            cursor.execute(
                "DELETE FROM \"new_order\" WHERE order_id = %s", (order_id,)
            )
            if cursor.rowcount == 0:
                cursor.close()
                return error.error_invalid_order_id(order_id)

            cursor.execute(
                "DELETE FROM \"new_order_detail\" where order_id = %s", (order_id,)
            )
            if cursor.rowcount == 0:
                cursor.close()
                return error.error_invalid_order_id(order_id)

            conn.commit()
            cursor.close()

        except psycopg2.Error as e:
            return 528, "{}".format(str(e))

        # except BaseException as e:
        #     return 530, "{}".format(str(e))

        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT password from \"user\" where user_id=%s", (user_id,)
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()

            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE \"user\" SET balance = balance + %s WHERE user_id = %s",
                (add_value, user_id),
            )
            if cursor.rowcount == 0:
                cursor.close()
                return error.error_non_exist_user_id(user_id)

            self.conn.commit()
            cursor.close()
        except psycopg2.Error as e:
            return 528, "{}".format(str(e))
        # except BaseException as e:
        #     return 530, "{}".format(str(e))

        return 200, "ok"
    
    
     # 添加收货功能
    def receive_order(self, order_id: str, user_id: str) -> (int, str):
        try:

            # 检查订单是否存在
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT user_id, status FROM \"new_order\" WHERE order_id = %s;", (order_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return error.error_invalid_order_id(order_id)
            # 检查用户是否为订单所有者
            if row[1] != user_id:
                return error.error_authorization_fail()

            if row[3] != 'paid':
                return error.error_order_status()
           # 更新订单状态为已收货
            cursor.execute(
                "UPDATE \"new_order\" SET status = 'received' WHERE order_id = %s;", (order_id,)
            )
            if cursor.rowcount == 0:
                return error.error_invalid_order_id(order_id)

            self.conn.commit()
            cursor.close()
            order_id = uid
            # if result.modified_count == 0:
            #     return error.error_invalid_order_id(order_id)
            
        except psycopg2.Error as e:
            return 528, "{}".format(str(e))
        return 200, "ok"
    
    # 添加定时任务来取消超时订单
    def cancel_timeout_orders(self):
        try:
            
            # 获取当前时间
            current_time = datetime.now(pytz.timezone('UTC'))

            # 获取超时时间阈值为15分钟
            timeout_threshold = timedelta(minutes=15)

            # 获取所有未付款的订单
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT order_id, store_id FROM \"new_order\" WHERE status = 'unpaid' AND order_date <= %s;", 
                (current_time - timeout_threshold,)
            )
            orders_to_cancel = cursor.fetchall()

            for order in orders_to_cancel:
                order_id = order[0]
                store_id = order[1]
                self.cancel_order(order_id, store_id)

            cursor.close()
            self.conn.commit()
            
            # order_col = self.db["new_order"]
            # unpaid_orders = order_col.find({"status": "unpaid"})

            # for order in unpaid_orders:
            #     # 获取订单创建时间
            #     order_date = order.get("order_date")

            #     # 计算订单创建时间到当前时间的时间差
            #     time_diff = current_time - order_date

            #     # 如果时间差超过超时时间阈值，则取消订单
            #     if time_diff >= timeout_threshold:
            #         order_id = order.get("order_id")
            #         # 执行取消订单操作
            #         self.cancel_order(order_id)
        except psycopg2.Error as e:
            return 528, "{}".format(str(e))
        return 200, "ok"
    
    def cancel_order(self, order_id: str) -> (int, str):
        try:
            # 检查订单是否存在
            # order_col = self.db["new_order"]
            # order_info = order_col.find_one({"order_id": order_id})
            # if order_info is None:
            #     return error.error_invalid_order_id(order_id)

            # # 将订单状态设置为已取消
            # result = order_col.update_one(
            #     {"order_id": order_id},
            #     {"$set": {"status": "cancelled"}}
            # )

            # # 返还订单中的书籍数量到商店库存
            # order_detail_col = self.db["new_order_detail"]
            # order_details = order_detail_col.find({"order_id": order_id})
            # store_col = self.db["store"]
            # for detail in order_details:
            #     book_id = detail.get("book_id")
            #     count = detail.get("count")
            #     store_col.update_one(
            #         {"store_id": order_info.get("store_id"), "book_id": book_id},
            #         {"$inc": {"stock_level": count}}
            #     )

            # # 删除订单详情记录
            # result = order_detail_col.delete_many({"order_id": order_id})

            # # 删除订单记录
            # result = order_col.delete_one({"order_id": order_id})
            conn = self.conn
            cursor = conn.cursor()
            cursor.execute(
                "SELECT order_id, user_id, store_id FROM \"new_order\" WHERE order_id = %s",
                (order_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return error.error_invalid_order_id(order_id)

            order_id = row[0]
            buyer_id = row[1]
            store_id = row[2]
            
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT book_id, count FROM \"new_order_detail\" WHERE order_id = %s;", (order_id,)
            )
            order_details = cursor.fetchall()

            for detail in order_details:
                book_id = detail[0]
                count = detail[1]
                cursor.execute(
                    "UPDATE \"store\" SET stock_level = stock_level + %s WHERE store_id = %s AND book_id = %s;",
                    (count, store_id, book_id)
                )

            cursor.execute(
                "DELETE FROM \"new_order_detail\" WHERE order_id = %s;", (order_id,)
            )
            cursor.execute(
                "DELETE FROM \"new_order\" WHERE order_id = %s;", (order_id,)
            )

            self.conn.commit()
            cursor.close()

        except psycopg2.Error as e:
            return 528, "{}".format(str(e))
        return 200, "ok"
    
       # 添加订单状态：
    def order_status(self, order_id: str) -> dict:
        try:
            # 检查订单是否存在
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT status FROM \"new_order\" WHERE order_id = %s;", (order_id, )
            )
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return json.dumps({"code": error.INVALID_ORDER_ID_CODE, "message": f"Invalid order id: {order_id}"})

            status = row[0]
        except psycopg2.Error as e:
            return json.dumps({"code": 528, "message": str(e)})
        return json.dumps({"code": 200, "message": "ok"})
    
    
    #添加订单查询功能
    def query_order(self, order_id: str) -> dict:
        try:
            # 检查订单是否存在
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT user_id, status, order_date, store_id FROM \"new_order\" WHERE order_id = %s;", 
                (order_id,)
            )
            order_info = cursor.fetchone()
            if order_info is None:
                cursor.close()
                return error.error_invalid_order_id(order_id) + ({},)
        

            order_status = order_info[1]
            order_date = order_info[2]
            store_id = order_info[3]

            cursor.execute(
                "SELECT book_id, count, price FROM \"new_order_detail\" WHERE order_id = %s;", 
                (order_id,)
            )
            order_details = cursor.fetchall()
            cursor.close()

            books = []
            total_price = 0
            for detail in order_details:
                book_id = detail[0]
                count = detail[1]
                price = detail[2]
                total_price += price * count
                books.append({
                    "book_id": book_id,
                    "count": count,
                    "price": price
                })

            order_dict = {
                "order_id": order_id,
                # "user_id": user_id,
                "store_id": store_id,
                "status": order_status,
                "order_date": order_date,
                "total_price": total_price,
                "books": books
            }
        except psycopg2.Error as e:
            return 528, "{}".format(str(e))
        return 200, "ok"