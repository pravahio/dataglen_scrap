# -*- coding: utf-8 -*-
# pip3 install mesh-solar-power-production
import scrapy
import json
import os
import datetime
import time
import logging

from mesh_solar_power_production.main import *
from mesh_solar_power_production.defaults import State
from mesh_solar_power_production import MeshRPCException

from scrapy.http import FormRequest, Request
from scrapy.exceptions import DontCloseSpider
from scrapy import signals

class GrowattSpider(scrapy.Spider):
    name = 'growatt'
    download_delay = 0.3
    allowed_domains = ['growatt.com']
    start_urls = ['https://oss.growatt.com/login']

    def __init__(self):
        self.mesh_client = MeshSolarPowerProduction('rpc.pravah.io:5555')

        self.geospace = [
            '/in/delhi'
        ]

        try:
            self.mesh_client.registerToPublish(self.geospace)
        except MeshRPCException as e:
            print(e.getMessage())
            exit()
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(GrowattSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def parse(self, response):
        #print(response.headers)
        #serverId = str(response.headers.getlist('Set-Cookie')[0]).split(';')[0]
        sessionId = str(response.headers.getlist('Set-Cookie')[0]).split(';')[0]

        self.cookie = sessionId

        #print(serverId)
        #print(sessionId)
        yield FormRequest('https://oss.growatt.com/login',
                            formdata = {'userName': os.environ['GROWATT_USERNAME'], # login
                                      'password': os.environ['GROWATT_PASSWORD']}, #password
                            headers = { 'Cookie': sessionId },
                            callback = self.start_scraping)
    
    def start_scraping(self, response):
        print(self.cookie)
        yield FormRequest('https://oss.growatt.com/deviceManage/inverterManage/list',
                            callback = self.parseInverterList,
                            headers = { 'Cookie': self.cookie + '; lang=en' },
                            formdata = {
                                'page': '1',
                                'order': '1',
                                'lineType': '0',
                                'deviceStatus': ''
                            },
                            meta = {'ps': {}, 'pn': {}},
                            dont_filter=True)
    
    def parseInverterList(self, response):
        jsonResponse = json.loads(response.body)
        #print(jsonResponse)
        
        plantSet = response.meta['ps']
        plantName = response.meta['pn']

        done = bool(plantSet)

        for d in jsonResponse['obj']['pagers'][0]['datas']:
            plant_id = d['plantId']
            #print(plant_id)

            timestamp = datetime.datetime.strptime(d['time'], '%Y-%m-%d %H:%M:%S')
            if float(d['etoday']) == 0.0:
                continue
            inv = {
                'id': d['uId'],
                'powerGenerationParameters': {
                    'currentPowerOutput': float(d['pac']),
                    'powerGeneratedToday': float(d['etoday'])
                },
                'info': {
                    'name': d['deviceSn']
                },
                'timestamp': int(timestamp.timestamp())
            }

            if d['status'] == '0' or d['status'] == '-1':
                inv['status'] = {
                    'state': State.DISCONNECTED
                }
            elif d['status'] == '1':
                inv['status'] = {
                    'state': State.CONNECTED
                }
            
            if plant_id in plantSet:
                plantSet[plant_id].append(inv)
            else:
                plantSet[plant_id] = [inv]
            
            if plant_id not in plantName:
                plantName[plant_id] = d['plantName']

        # No need to publish if we received no data.
        if not plantSet:
            return

        if not done:
            yield FormRequest('https://oss.growatt.com/deviceManage/inverterManage/list',
                                callback = self.parseInverterList,
                                headers = { 'Cookie': self.cookie + '; lang=en' },
                                formdata = {
                                    'page': '2',
                                    'order': '1',
                                    'lineType': '0',
                                    'deviceStatus': ''
                                },
                                meta = {'ps': plantSet, 'pn': plantName},
                                dont_filter=True)
        else:
            self.publish_data(plantName, plantSet)
    
    def publish_data(self, pn, ps):
        stations = []

        for station_id, inv_details in ps.items():
            station_status = {
                'state': State.DISCONNECTED
            }
            for inv in inv_details:
                if inv['status'] == State.CONNECTED:
                    station_status = {
                        'state': State.CONNECTED
                    }
                    break

            stations.append({
                'id': station_id,
                'info': {
                    'name': pn[station_id]
                },
                'inverterList': inv_details,
                'status': station_status
            })
        
        message = {
            'header': {
                'version': '0.0.1'
            },
            'stations': stations
        }
        print('Publishing for: ' + str(pn.values()))

        try:
            self.mesh_client.publish(self.geospace, message)
        except MeshRPCException as e:
            print(e.getMessage())
            exit()
    
    def spider_idle(self, spider):
        time.sleep(10)

        logging.info('starting a crawl again!')

        for req in self.start_scraping(None):
            self.crawler.engine.schedule(req, spider)
        raise DontCloseSpider
