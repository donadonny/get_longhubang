# __author__ = 'fit'
# -*- coding: utf-8 -*-
from ..model import *
import scrapy
from ..items import *
import urllib
import re
import MySQLdb


class get_content_spider(scrapy.Spider):
    name = "get_content"
    allowed_domains = ["eastmoney.com"]

    def get_lastest_date(self):
        con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8", db="")

    # 获取链接
    def build_url_by_date(self, prev_date=True):
        import datetime
        if prev_date is True:
            prev_date = datetime.datetime.today().date() + datetime.timedelta(-1)
        else:
            prev_date = datetime.datetime.today().date()
        prev_date = datetime.datetime(2016, 05, 12).date()
        html_url = '''http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=%s,endDate=%s''' % (
            prev_date, prev_date)
        html_url += ",gpfw=0,js=var%20data_tab_1.html"
        wp = urllib.urlopen(html_url)
        str = wp.read()
        code_list = re.findall(r"SCode\":\"\d\d\d\d\d\d", str)
        i = 0
        while i < len(code_list):
            code_list[i] = code_list[i][-6:]
            i += 1
        ret = []
        if code_list is None or len(code_list) == 0:
            return ret
        # 对当天的股票代码进行去重处理
        code_list = list(set(code_list))
        # 第一次检查重复
        for stock_id in code_list:
            url = '''http://data.eastmoney.com/stock/lhb,%s,%s.html''' % (prev_date, stock_id)
            ret.append(url)
        return ret

    def start_requests(self):
        code_list = self.build_url_by_date();
        for item in code_list:
            yield self.make_requests_from_url(item)

    def parse(self, response):
        stock_code = response.url.split(',')[2][0:6]
        date = response.url.split(',')[1]
        list = response.xpath("//div[@class='left con-br']/text()").extract()
        if list is None or len(list) == 0:
            url_one = url.select().where(url.url == response.url).first()
            print '该链接无效'
            url_one.processed = -1
            url_one.save()
            return
        table1s = response.xpath("//table[@class='default_tab stock-detail-tab']")
        table2s = response.xpath("//table[@class='default_tab tab-2']")
        for i in range(0, len(table1s)):
            table1 = table1s[i].xpath(".//tbody/tr")
            reason = None
            if i < len(list):
                reason = list[i]
                reason = reason.encode("utf8")
                reason = reason.split("：")[1]
            for one in table1:
                tmp = one.xpath("./td/text()").extract()
                if len(tmp) < 10:
                    break
                department = one.xpath("./td/div[@class='sc-name']/a/text()").extract()
                item = GetContentItem()
                item['tag'] = 1  # 1 买入
                if department is None or len(department) == 0:
                    item['department'] = None
                else:
                    item['department'] = department[0].encode("utf8")
                item['date'] = date
                item['stock_code'] = stock_code
                item['reason'] = reason
                item['serial_number'] = tmp[0]
                item['buy'] = tmp[5]
                item['buy_percent'] = tmp[6]
                item['sell'] = tmp[7]
                item['sell_percent'] = tmp[8]
                item['net'] = tmp[9]
                yield item
        for i in range(0, len(table2s)):
            table2 = table2s[i].xpath(".//tbody/tr")
            reason = None
            if i < len(list):
                reason = list[i]
                reason = reason.encode("utf8")
                reason = reason.split("：")[1]
            for one in table2:
                tmp = one.xpath("./td/text()").extract()
                if len(tmp) < 9:
                    break
                department = one.xpath("./td/div[@class='sc-name']/a/text()").extract()
                item = GetContentItem()
                item['tag'] = 2  # 2 卖出
                if (department is None or len(department) == 0):
                    item['department'] = None
                else:
                    item['department'] = department[0].encode("utf8")
                item['date'] = date
                item['stock_code'] = stock_code
                item['reason'] = reason
                item['serial_number'] = tmp[0]
                item['buy'] = tmp[4]
                item['buy_percent'] = tmp[5]
                item['sell'] = tmp[6]
                item['sell_percent'] = tmp[7]
                item['net'] = tmp[8]
                yield item
        url_one = url(processed=1, url=response.url)
        url_one.save()
