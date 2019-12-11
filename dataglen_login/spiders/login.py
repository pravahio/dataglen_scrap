# -*- coding: utf-8 -*-
import scrapy
import json

from scrapy.http import FormRequest

class LoginSpider(scrapy.Spider):
    name = 'login'
    allowed_domains = ['dataglen.com']
    start_urls = ['https://dataglen.com/api-auth/login/']

    def parse(self, response):
        csrf_token = response.xpath('//*[@name = "csrfmiddlewaretoken"]/@value').extract_first()
        yield FormRequest('https://dataglen.com/api-auth/login/',
                          formdata = {'csrfmiddlewaretoken': csrf_token,
                                      'username': '******', # login
                                      'password': '******'}, #password
                          callback = self.start_scraping)

    def start_scraping(self, response):
        json_url = 'https://dataglen.com/api/v1/solar/plants/?format=json'
        yield FormRequest(json_url, callback = self.parse_form)

    def parse_form(self, response):
        jsonresponse = json.loads(response.body)
        slug_name = [name['slug'] for name in jsonresponse]
        next_urls = ["https://dataglen.com/api/v1/solar/plants/{}/summary/?format=json".format(name) for name in slug_name]
        for link in next_urls:
            yield FormRequest(link, callback = self.json_scrap)

    def json_scrap(self, response):
        jsonresponse = json.loads(response.body)
        yield {'plant_name': jsonresponse['plant_name'],
               'plant_slug':jsonresponse['plant_slug'],
               'longitude':jsonresponse['longitude']
               }
