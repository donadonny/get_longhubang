# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import re
from model import *


class transformPipeline(object):
    def __init__(self):
        self.pattern = re.compile(r'^(-?\d+)(\.\d+)?$')

    def process_item(self, item, spider):
        item['date'] = datetime.datetime.strptime(item['date'], "%Y-%m-%d").date()
        if self.pattern.match(item['buy']):
            item['buy'] = float(item['buy'])
        else:
            item['buy'] = None
        if self.pattern.match(item['sell']):
            item['sell'] = float(item['sell'])
        else:
            item['sell'] = None
        if self.pattern.match(item['net']):
            item['net'] = float(item['net'])
        else:
            item['net'] = None
        if len(item['buy_percent']) <= 1:
            item['buy_percent'] = None
        else:
            item['buy_percent'] = item['buy_percent'][:-1]
            item['buy_percent'] = float(item['buy_percent'])

        if len(item['sell_percent']) <= 1:
            item['sell_percent'] = None
        else:
            item['sell_percent'] = item['sell_percent'][:-1]
            item['sell_percent'] = float(item['sell_percent'])
        if self.pattern.match(item['serial_number']):
            item['serial_number'] = int(item['serial_number'])
        else:
            item['serial_number'] = None
        return item


class GetContentPipeline(object):
    def process_item(self, item, spider):
        # 修改为直接插入，upsert操作太费时
        one = longhubang(
            stock_code=item['stock_code'],
            date=item['date'],
            reason=item['reason'],
            department=item['department'],
            sell=item['sell'],
            sell_percent=item['sell_percent'],
            buy=item['buy'],
            buy_percent=item['buy_percent'],
            tag=item['tag'],
            net=item['net'],
            serial_number=item['serial_number'],
        )
        one.save()
        return item
