#!/home/qy/vyper-venv/bin/python
# -*- coding: utf-8 -*-
"""
规则的设计标准，尽量兼容性
"""
import pymysql

#阈值型
def singer_limit_rules(db_conn,table,limit_obj,limit_value):
    """

    :param db_conn:
    :param limit_obj:
    :param limit_value:
    :return:
    """
    sql_str = ""
    cursor = db_conn.cursor()
    cursor.execute(
    "select hash from {} where {} > {};".format(table, limit_obj, limit_value)
    )
    results = cursor.fetchall()
    db_conn.commit()
    cursor.close()

if __name__ == '__main__':
    db_name = "eth_data"
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')
    singer_limit_rules(db_conn, "transactions", "gasPrice",9000000000)

