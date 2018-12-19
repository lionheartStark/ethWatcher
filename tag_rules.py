#!/home/qy/vyper-venv/bin/python
# -*- coding: utf-8 -*-
from web3 import Web3
import sqlite3,re,time,threadpool
import requests
"""
规则的设计标准，尽量兼容性
"""

#阈值型
def limit_rules(db_conn,limit_obj,limit_value):
    """

    :param db_conn:
    :param limit_obj:
    :param limit_value:
    :return:
    """
    sql_str = ""
    c = db_conn.cursor()
    c.execute(sql_str)

def c():
    """

    :return:
    """