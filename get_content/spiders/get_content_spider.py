# __author__ = 'fit'`
# -*- coding: utf-8 -*-
from get_content.model import *
from get_content.items import *
import urllib
import re
import MySQLdb
import pandas as pd
import scrapy


class get_content_spider(scrapy.Spider):
    name = "get_content"
    allowed_domains = ["eastmoney.com"]

    # 获取数据表中最晚的那天的日期
    def get_lastest_date(self):
        con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                              db="pachong")
        cur = con.cursor()
        sql = "select date from longhubang order by date desc limit 0,1"
        cur.execute(sql)
        result = cur.fetchone()
        cur.close()
        con.close()
        if result is None or len(result) == 0:
            print 'maybe table longhubang is wrong!'
            exit(-1)
        return result[0]

    def build_url_by_loss_date(self):
        latest_date = self.get_lastest_date()
        update_con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                                     db="update")
        sql = "select distinct SH_tradeday as date from tradeday where SH_tradeday >'%s' " % latest_date
        df = pd.read_sql(sql, con=update_con)
        df['date'] = df['date'].map(lambda x: x.to_datetime().date())
        date_list = df['date'].tolist()
        ret_urls = []
        i = 1.0
        length = len(date_list)
        for date in date_list:
            print "build urls", i / length, "complete!"
            html_url = '''http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=%s,endDate=%s''' % (
                date, date)
            html_url += ",gpfw=0,js=var%20data_tab_1.html"
            wp = urllib.urlopen(html_url)
            content = wp.read()
            code_list = re.findall(r"SCode\":\"\d\d\d\d\d\d", content)
            code_list = list(set(code_list))
            code_list = map(lambda x: x[-6:], code_list)
            url_list = map(lambda x: '''http://data.eastmoney.com/stock/lhb,%s,%s.html''' % (date, x), code_list)
            ret_urls.extend(url_list)
            i += 1
        return ret_urls

    def start_requests(self):
        url_list = self.build_url_by_loss_date();
        for url in url_list:
            yield self.make_requests_from_url(url)

    def parse(self, response):
        stock_code = response.url.split(',')[2][0:6]
        date = response.url.split(',')[1]
        list = response.xpath("//div[@class='left con-br']/text()").extract()
        if list is None or len(list) == 0:
            print '该链接无效'
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

