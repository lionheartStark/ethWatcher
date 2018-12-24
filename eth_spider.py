#!/home/qy/vyper-venv/bin/python
# -*- coding: utf-8 -*-
"""
规则的设计标准，尽量兼容性
"""
import urllib
import urllib.request
import urllib.parse
import urllib.error
import re
import pymysql
import threadpool


def know_page_num(url):
    respond_str = spider_one_page(url)
    number = find_from_str(respond_str, "Page <b>1</b> of <b>([0-9]+)</b>")
    return number


def spider_one_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    req = urllib.request.Request(url=url, headers=headers)
    respond = urllib.request.urlopen(req).read()
    respond_str = str(respond, encoding="utf-8")
    return respond_str


def find_from_str(html_str, find_key):
    token_url_list = re.findall(find_key, html_str)
    token_url_list = sorted(set(token_url_list), key=token_url_list.index)
    # print(token_url_list)
    return token_url_list


def create_table(db_name):
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')

    c = db_conn.cursor()
    c.execute('''
    create table if not exists tokens (
        name varchar(50) primary key not null,
        total_supply varchar(200)  not null,
        owner varchar(100) not null
    );
    ''')

    print("Table created successfully")
    db_conn.commit()
    c.close()
    db_conn.close()


def find_token_detail(url_str, token_name):
    url = "https://etherscan.io/readContract?a={}&v={}".format(url_str, url_str)
    detail_str = spider_one_page(url)
    total_supply = find_from_str(detail_str, ">totalSupply<.*?class='form-group'>([0-9]+[^<]*)<")
    owner = find_from_str(detail_str, ">owner<.*?href='/address/([^']*)'")

    # token_detail += find_from_str(detail_str, "href='/address/(0x[^']*)'")
    # print(token_name, "\n", total_supply, "\n", owner, "\n========================================\n")

    return total_supply+owner


def save_details(db_name, token_name, total_supply, owner):
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')
    c = db_conn.cursor()
    sql_str = "insert into tokens  values('{}','{}','{}')".format(token_name, total_supply, owner)
    c.execute(sql_str)
    c.close()
    db_conn.commit()
    db_conn.close()


def do_prejobs(start_num, limit_num):
    print("%d to %d" % (start_num, start_num + limit_num))
    for i in range(limit_num):
        num = start_num + i
        print("num = {}".format(num))
        my_html_str = spider_one_page(my_url + "?p=" + str(num))
        one_page_url_list = find_from_str(my_html_str, "href='/token/(0x[^']*)'>([^<]+)<")
        for item in one_page_url_list:
            # print(i, "\n")
            # item[0]是代币合约地址，[1]是代币名称
            detail = find_token_detail(item[0], item[1])
            name_and_detail = [item[1]] + detail
            if len(name_and_detail) is 3:
                save_details(my_db, name_and_detail[0], name_and_detail[1], name_and_detail[2])
            i += 1
    return


# 从start_num开始收集limit_num个区块的数据
def mutil_thread_get_data(start_num, limit_num, thread_num):
    all_jobs = limit_num
    prejobs = int((all_jobs-all_jobs % thread_num)/thread_num)
    lastjobs = prejobs+all_jobs % thread_num
    list_of_args = []
    for i in range(thread_num-1):
        list_of_args.append(([start_num+i*prejobs, prejobs], None))
    list_of_args.append(([start_num + (thread_num-1) * prejobs, lastjobs], None))
    print(list_of_args)
    pool = threadpool.ThreadPool(thread_num)
    requests = threadpool.makeRequests(do_prejobs, list_of_args)
    [pool.putRequest(req) for req in requests]
    pool.wait()
    return


def drop_table(table_name,db_name):
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')
    c = db_conn.cursor()
    sql_str = "drop table {}".format(table_name)
    c.execute(sql_str)
    c.close()
    db_conn.commit()
    db_conn.close()


if __name__ == '__main__':

    print("start")
    my_url = "https://etherscan.io/tokens"
    my_db = "eth_data"

    page_num = int(know_page_num(my_url)[0])

    drop_table("tokens", my_db)
    create_table(my_db)

    mutil_thread_get_data(1, page_num, 8)
    # i = 1
    # for num in range(page_num):
    #     my_html_str = spider_one_page(my_url+"?p="+str(num+1))
    #     one_page_url_list = find_from_str(my_html_str, "href='/token/(0x[^']*)'>([^<]+)<")
    #     for item in one_page_url_list:
    #         print(i, "\n")
    #         # item[0]是代币合约地址，[1]是代币名称
    #         detail = find_token_detail(item[0], item[1])
    #         name_and_detail = [item[1]] + detail
    #         if len(name_and_detail) is 3:
    #             save_details(my_db, name_and_detail[0], name_and_detail[1], name_and_detail[2])
    #         i += 1




