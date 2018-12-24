#!/home/qy/vyper-venv/bin/python
# -*- coding: utf-8 -*-
import re
import threadpool
import requests
import pymysql
import time

URL = 'https://mainnet.infura.io/'
session = requests.Session()

''''========================== DB  FUNCTION ========================== '''


# 数据库建表操作
def create_table(db_conn):
    c = db_conn.cursor()
    c.execute('''
    create table if not exists transactions (
        hash varchar(66) primary key not null,
        
        blockHash varchar(66) not null,
        blockNumber bigint not null,
        
        from_ varchar(66) not null,
        to_ varchar(66) not null,

        gas bigint  not null,
        gasPrice bigint not null,
        value varchar(66) not null,
        
        
        nonce varchar(66) not null,
        transactionIndex varchar(66) not null,
        
        
        tag int default 0 not null
    );
    ''')

    print("Table created successfully")
    db_conn.commit()
    c.close()


# 执行一个sql语句
def do_sql(db_conn, sql_str):
    c = db_conn.cursor()
    c.execute(sql_str)
    # db_conn.commit()
    c.close()
    return


# 将块中全部交易提交db
def blk_trans_to_db(input_blk,db_conn):
    if input_blk['transactions'] is []:
        return
    for transaction in input_blk['transactions']:
        sql_values = ""
        sql_items = ""
        for item in transaction:
            # 存为int型的属性
            if item in ["gasPrice", "gas", "blockNumber"]:
                # print("\'"+web3.toHex((transaction[item]))+"\'")
                sql_items += item + ","
                sql_values += str(int((transaction[item]), 16))+","
            # 存为str型的属性
            elif item in ["hash", "nonce", "blockHash", "transactionIndex", "value",
                          "from", "to"]:
                if item in ["from", "to"]:
                    sql_items += item + "_,"
                else:
                    sql_items += item + ","
                sql_values += "'"+str(transaction[item])+"',"
        # 因为末尾多了一个逗号，所以需要删除
        sql_items = sql_items[0:-1]
        sql_values = sql_values[0:-1]
        sql_str = "insert into transactions ("+sql_items+") " + "values("+sql_values+")"
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
    block = rpc_func('eth_getBlockByNumber', [_BlockNum, True], _requestId)
    if block is None:
        block = RequestData(_BlockNum,_requestId)
    return block


# 对每个子线程来说的任务
def do_prejobs(thread_num,start_num, limit_num):
    print("%d to %d"%(start_num,start_num+limit_num))
    db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')
    for i in range(limit_num):
        start_num_i = start_num+i
        latest_blk = RequestData(str(hex(start_num_i)), requestId)
        blk_trans_to_db(latest_blk, db_conn)
        time.sleep(0.01)
    db_conn.commit()
    db_conn.close()
    return


# 从start_num开始收集limit_num个区块的数据
def get_data_force_end(start_num, limit_num, thread_num):
    # 获取当前块号
    # 从start_num块开始，收集limit_num个块数据用于训练
    # 划分线程提高效率
    force_end_num = int((rpc_func('eth_blockNumber', [], requestId)), 16)
    all_jobs = force_end_num-start_num+1

    if all_jobs <= 0:
        print("we can not get future data")
    else:
        if all_jobs > limit_num & limit_num != 0:
            print("too much jobs,change all_jobs to limit")
            all_jobs = limit_num
        prejobs = int((all_jobs-all_jobs % thread_num)/thread_num)
        lastjobs = prejobs+all_jobs % thread_num
        list_of_args = []
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


def drop_table(db_conn):
    c = db_conn.cursor()
    c.execute('''
    drop table  transactions;
    ''')
    print("Table clean successfully")
    db_conn.commit()
    c.close()


if __name__ == '__main__':
    db_name = "eth_data"
    create_db(db_name)
    print("create_db ok!")
    my_db_conn = pymysql.connect("localhost", "root", "789826", db_name, charset='utf8')

    drop_table(my_db_conn)
    create_table(my_db_conn)

    requestId = 1
    force_end_num1 = int((rpc_func('eth_blockNumber', [], requestId)), 16)

    get_data_force_end(force_end_num1-10, 3, 1)
    my_db_conn.commit()
    my_db_conn.close()







