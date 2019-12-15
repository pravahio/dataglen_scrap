# -*- coding: utf-8 -*-
import scrapy
import json
import os
import datetime
import time

from mesh_solar_power_production.main import *
from mesh_solar_power_production import MeshRPCException

from scrapy.http import FormRequest, Request

class LoginSpider(scrapy.Spider):
    name = 'login'
    download_delay = 0.3
    allowed_domains = ['dataglen.com']
    start_urls = ['https://dataglen.com/api-auth/login/']

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

    def parse(self, response):
        csrf_token = response.xpath('//*[@name = "csrfmiddlewaretoken"]/@value').extract_first()
        yield FormRequest('https://dataglen.com/api-auth/login/',
                          formdata = {'csrfmiddlewaretoken': csrf_token,
                                      'username': os.environ['DATAGLEN_USERNAME'], # login
                                      'password': os.environ['DATAGLEN_PASSWORD']}, #password
                          callback = self.start_scraping)

    def start_scraping(self, response):
        json_url = 'https://dataglen.com/api/v1/solar/plants/?format=json'
        yield Request(json_url, callback = self.parse_form)

    def parse_form(self, response):
        jsonresponse = json.loads(response.body)
        slug_list = [name['slug'] for name in jsonresponse]
        while(True):
            for slug_name in slug_list:
                url = "https://dataglen.com/api/v1/solar/plants/{}/summary/?format=json".format(slug_name)
                yield Request(url, callback = self.json_scrap)
            time.sleep(2)

    def json_scrap(self, response):
        json_response = json.loads(response.body)

        # TODO: Might be empty.
        gen_today = [float(i) for i in json_response['plant_generation_today'].split() if i.isdigit()]
        plant_capacity = [float(i) for i in json_response['plant_capacity'].split() if i.isdigit()]
        timestamp = datetime.datetime.strptime(json_response['updated_at'], '%Y-%m-%dT%H:%M:%S.%fZ')

        station = {
            'id': json_response['plant_slug'],
            'powerGenerationParameters': {
                'currentPowerOutput': json_response['current_power'],
                'irradiation': json_response['irradiation'],
                'moduleTemperature': json_response['module_temperature']
            },
            'info': {
                'name': json_response['plant_name'],
                'location': {
                    'latitude': json_response['latitude'],
                    'longitude': json_response['longitude']
                }
            },
            'timestamp': int(timestamp.timestamp())
        }

        if len(gen_today) > 1:
            station['powerGenerationParameters']['powerGeneratedToday'] = gen_today[0]
        
        if len(plant_capacity) > 1:
            station['info']['plant_capacity[0]'] = plant_capacity[0]

        url = "https://dataglen.com/api/v1/solar/plants/{}/live/?format=json".format(json_response['plant_slug'])
        yield Request(url, callback = self.scrap_inverter_details, meta={'station': station})
    
    def scrap_inverter_details(self, response):
        inverter_list = []

        json_response = json.loads(response.body)

        for inv in json_response['inverters']:
            timestamp = datetime.datetime.strptime(inv['last_timestamp'], '%Y-%m-%dT%H:%M:%SZ')

            inv_obj = {
                'id': inv['name'],
                'powerGenerationParameters': {
                    'currentPowerOutput': inv['power'],
                    'powerGeneratedToday': inv['generation'],

                },
                'info': {
                    'name': inv['name'],
                    'powerCapacity': inv['capacity']
                },
                'timestamp': int(timestamp.timestamp())
            }
            inverter_list.append(inv_obj)
        
        station = response.meta['station']
        station['inverterList'] = inverter_list

        for inv in inverter_list:
            url = 'https://dataglen.com/api/v1/solar/plants/{}/live/?device_type=inverter&inverter={}&format=json'.format(station['id'], inv['id'])
            yield Request(url, callback = self.scrap_inv_phase_components,
            meta={'station': station})
    
    def scrap_inv_phase_components(self, response):
        station = response.meta['station']

        message = {
            'header': {
                'version': '0.0.1'
            },
            'stations': [station]
        }

        try:
            self.mesh_client.publish(self.geospace, message)
            print('Publishing #')
        except MeshRPCException as e:
            print(e.getMessage())
            exit()
