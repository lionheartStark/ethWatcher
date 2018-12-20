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


def know_page_num(url):
    respond_str = spider_one_page(url)
    num = find_from_str(respond_str, "Page <b>1</b> of <b>([0-9]+)</b>")
    return num


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


def find_token_detail(url_str, token_name):
    url = "https://etherscan.io/readContract?a={}&v={}".format(url_str, url_str)
    detail_str = spider_one_page(url)
    total_supply = find_from_str(detail_str, ">totalSupply<.*?class='form-group'>([0-9]+[^<]*)<")
    owner = find_from_str(detail_str, ">owner<.*?href='/address/([^']*)'")

    # token_detail += find_from_str(detail_str, "href='/address/(0x[^']*)'")
    print(token_name, "\n", total_supply, "\n", owner, "\n========================================\n")

    return


if __name__ == '__main__':
    print("start")
    my_url = "https://etherscan.io/tokens"

    page_num = int(know_page_num(my_url)[0])
    i = 1
    for num in range(page_num):
        my_html_str = spider_one_page(my_url+"?p="+str(num+1))
        one_page_url_list = find_from_str(my_html_str, "href='/token/(0x[^']*)'>([^<]+)<")
        for item in one_page_url_list:
            print(i, "\n")
            find_token_detail(item[0], item[1])
            i += 1






