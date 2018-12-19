#!/home/qy/vyper-venv/bin/python
# -*- coding: utf-8 -*-
import sqlite3, re, time, threadpool
import requests
import pymysql

URL = 'https://mainnet.infura.io/'
session = requests.Session()

''''========================== DB  FUNCTION ========================== '''


# 数据库建表操作
def create_table(db_conn):
    c = db_conn.cursor()
    c.execute('''
    create table if not exists transactions (
        hash varchar(100) primary key not null,
        gasPrice bigint  not null
    );
           ''')

    print("Table created successfully")
    db_conn.commit()
    c.close()


# 执行一个sql语句
def do_sql(db_conn, sql_str):
    c = db_conn.cursor()
    c.execute(sql_str)
    # print("execute %s \n"%sql_str)
    #
    return


# 将块中全部交易提交
def blk_trans_to_db(input_blk):
    if input_blk['transactions']==[]:
        return
    for transaction in input_blk['transactions']:
        sql_values=""
        sql_items=""
        for item in transaction:
            if (item in ["gasPrice"]):
                # print("\'"+web3.toHex((transaction[item]))+"\'")
                sql_items += item + ","
                sql_values += str(int((transaction[item]), 16))+","
            elif (item in ["hash"]):
                sql_items += item + ","
                sql_values += transaction[item]+","
        # 因为末尾多了一个逗号，所以需要删除
        sql_items = sql_items[0:-1]
        sql_values = sql_values[0:-1]
        sql_str = "insert into transactions ("+sql_items+") "+ "values("+sql_values+")"
        print(sql_str, "\n")
        do_sql(db_conn, sql_str)
    return


# 判断是否已经建表
def table_not_exists(con, table_name):
    sql = "show tables;"
    cursor = con.cursor()
    cursor.execute(sql)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    cursor.close()
    if table_name in table_list:
        return 0
    else:
        return 1



'''========================== RPC FUNCTION ========================== '''


def createJSONRPCRequestObject(_method, _params, _requestId):
    return {"jsonrpc": "2.0",
            "method": _method,
            "params": _params,
            "id": _requestId}


def postJSONRPCRequestObject(_HTTPEnpoint, _jsonRPCRequestObject):
    response = session.post(_HTTPEnpoint,
                            json=_jsonRPCRequestObject,
                            headers={'Content-type': 'application/json'})

    return response.json()


def rpc_func(_apiMethod, _apiParameter, _rpcRequestId):
    requestObject = createJSONRPCRequestObject(_apiMethod, _apiParameter, _rpcRequestId)
    responseObject = postJSONRPCRequestObject(URL, requestObject)
    receipt = responseObject['result']
    _rpcRequestId+=1
    return receipt


'''========================== DO JOB FUNCTION ========================== '''


# 请求块数据
def RequestData(_BlockNum,_requestId):
    Block = rpc_func('eth_getBlockByNumber', [_BlockNum, True], _requestId)
    if Block == None:
        Block = RequestData(_BlockNum,_requestId)
    return Block


# 对每个子线程来说的任务
def do_prejobs(thread_num,start_num, limit_num):
    print("%d to %d"%(start_num,start_num+limit_num))
    for i in range(limit_num):
        start_num_i=start_num+i
        latest_blk = RequestData(str(hex(start_num_i)), requestId)
        blk_trans_to_db(latest_blk)
    return


# 从start_num开始收集limit_num个区块的数据
def get_data_force_end(start_num, limit_num, thread_num):
    # 获取当前块号
    # 从start_num块开始，收集limit_num个块数据用于训练
    # 划分线程提高效率
    force_end_num = int((rpc_func('eth_blockNumber', [], requestId)), 16)
    all_jobs = force_end_num-start_num+1
    if (all_jobs <= 0):
        print("we can not get future data")
    else:
        if (all_jobs > limit_num & limit_num != 0):
            print("too much jobs,change all_jobs to limit")
            all_jobs = limit_num
        prejobs = int((all_jobs-all_jobs % thread_num)/thread_num)
        lastjobs = prejobs+all_jobs % thread_num
        list_of_args=[]
        for i in range(thread_num-1):
            list_of_args.append(([i, start_num+i*prejobs, prejobs], None))
        list_of_args.append(([thread_num-1, start_num + (thread_num-1) * prejobs, lastjobs], None))
        print(list_of_args)
        pool = threadpool.ThreadPool(thread_num)
        requests = threadpool.makeRequests(do_prejobs, list_of_args)
        [pool.putRequest(req) for req in requests]
        pool.wait()
    return


def create_db(name):
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='789826', charset='utf8')
    cursor = conn.cursor()
    cursor.execute("create database if not exists %s" % name)
    conn.select_db(name)
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    db_name = "eth_data"
    create_db(db_name)
    print("create_db ok!")
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')

    create_table(db_conn)
    requestId = 1
    force_end_num1 = int((rpc_func('eth_blockNumber', [], requestId)), 16)

    get_data_force_end(force_end_num1-12, 3, 3)
    db_conn.commit()
    db_conn.close()

    # https: // github.com / ethereum / go - ethereum / wiki / Management - APIs






