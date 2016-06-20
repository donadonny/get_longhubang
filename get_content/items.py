# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class GetContentItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    date =scrapy.Field()
    stock_code =scrapy.Field()
    department=scrapy.Field()
    buy=scrapy.Field()
    buy_percent =scrapy.Field()
    sell =scrapy.Field()
    sell_percent =scrapy.Field()
    net=scrapy.Field()
    tag=scrapy.Field()
    serial_number = scrapy.Field()
    reason = scrapy.Field()


