# import sqlite3 as sqlite
import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import psycopg2


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
