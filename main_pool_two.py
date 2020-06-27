import logging
import random
import time

import requests
from multiprocessing.pool import ThreadPool
from openpyxl import Workbook
from exel_parser_number import xlsx_data_parsing, one_number_get


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('PARSING')


def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        ended_at = time.time()
        elapsed = round(ended_at - started_at)
        print('')
        print(f'Time run func {elapsed} sec.')
        return result
    return surrogate


class Parser:

    def __init__(self, number):
        self.number = number
        self.session = requests.Session()
        self.session.headers = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv: 77.0) Gecko/20100101 Firefox / 77.0',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Content-Type': 'application/json'
        }
        self.address = ''
        self.c_price = None  # float
        self.area = None  # float
        self.id_land = None  # int
        self.id_lands = []
        self.cn_land = None  # int
        self.title_land = None
        self.year_2017 = ''
        self.year_2018 = ''
        self.year_2019 = ''
        self.year_2020 = ''
        self.date_check = ''
        self.date_result = ''

        self.id_number_cad = None
        self.data_for_record = []
        self.error_numbers = []

        self.proxies_one = {
            'https': 'https://login:pass@ip:8000/'
        }
        self.proxies_two = {
            'https': 'https://login:pass@ip:8000/'
        }
        self.data_json = {
            "cadastralNumber": self.number,
            }

        self.site_1_domain_one = 'https://pkk.rosreestr.ru/api/features/5/{}'
        self.site_1_domain_two = 'https://pkk.rosreestr.ru/api/features/1?sqo={}&sqot=5'
        self.site_1_domain_three = 'https://pkk.rosreestr.ru/api/features/1/{}'

        # self.site_2_domain_one = 'https://tr.mos.ru/widget-niok/api/details/cadsearch'
        # self.site_2_domain_two = 'https://tr.mos.ru/widget-niok/api/details/byid'
        #
        # self.site_3_domain_one = 'https://tr.mos.ru/widget-gin/api/ginobjects'

    def loading(self, url, post=False, data=None, json=None):
        while True:
            try:
                time.sleep(random.randint(1, 3))
                if not post:
                    result = self.session.get(url=url, proxies=self.proxies_one, timeout=10)
                else:
                    result = self.session.post(url=url, data=data, json=json, proxies=self.proxies_two, timeout=10)
                data = result.json()
                return data
            except Exception as exp:
                time.sleep(random.randint(2, 5))
                logger.info(exp)

    def site_1_parser_one(self, result):
        # self.address = result['feature']['attrs']['address']
        self.id_number_cad = result['feature']['attrs']['id']
        # self.c_price = result['feature']['attrs']['cad_cost']
        # self.area = result['feature']['attrs']['area_value']

        return self.id_number_cad

    def site_1_parser_two(self, result):
        for idx in result['features']:
            self.id_lands.append(
                idx['attrs']['id'],
            )
        return self.id_lands

    def site_1_parser_three(self, result):
        self.id_land = result['feature']['attrs']['cn']
        self.title_land = result['feature']['attrs']['util_by_doc']

    # @staticmethod
    # def site_2_parser_one(result):
    #     id_land = result['point'][0]['id']
    #     return id_land
    #
    # def site_2_parser_two(self, result):
    #     for item in result:
    #         id_data = item['year']
    #         came_under_taxation_string = item['cameUnderTaxationString']
    #         self._check_in_date(id_data, came_under_taxation_string)
    #
    # def site_3_parser_one(self, result):
    #     if result['ginObjects']:
    #         self.date_check = result['ginObjects'][0]['dateEvent']
    #         self.date_result = result['ginObjects'][0]['result']
    #
    # def _check_in_date(self, date, data):
    #     if date == 2020:
    #         self.year_2020 = data
    #     elif date == 2019:
    #         self.year_2019 = data
    #     elif date == 2018:
    #         self.year_2018 = data
    #     elif date == 2017:
    #         self.year_2017 = data

    def run(self):

        try:
            id_data = self._correct_number()
            self.site_1_run(id_data)
        except Exception as exp:
            logger.info(exp)

        # try:
        #     self.site_2_run()
        # except Exception as exp:
        #     logger.info(exp)
        #
        # try:
        #     self.site_3_run()
        # except Exception as exp:
        #     logger.info(exp)

        print(self.data_for_record)

    def site_1_run(self, id_data):
        site_1_result = self.loading(self.site_1_domain_one.format(id_data))
        id_number = self.site_1_parser_one(result=site_1_result)

        site_1_result_two = self.loading(self.site_1_domain_two.format(id_number))
        id_lands = self.site_1_parser_two(result=site_1_result_two)

        for id_land in id_lands:
            site_1_result_three = self.loading(self.site_1_domain_three.format(id_land))
            self.site_1_parser_three(result=site_1_result_three)

            self.data_for_record.append([self.number, self.id_land, self.title_land])

    # def site_2_run(self):
    #     site_2_result_one = self.loading(url=self.site_2_domain_one, json=self.data_json, post=True)
    #     id_number = self.site_2_parser_one(result=site_2_result_one)
    #
    #     site_2_result_two = self.loading(url=self.site_2_domain_two, data=str(id_number), post=True)
    #     self.site_2_parser_two(result=site_2_result_two)
    #
    # def site_3_run(self):
    #     site_3_result_one = self.loading(url=self.site_3_domain_one, json=self.data_json, post=True)
    #     self.site_3_parser_one(result=site_3_result_one)

    def _correct_number(self):
        chars = self.number.split(':')
        chars[1] = str(int(chars[1]))
        chars[2] = str(int(chars[2]))
        id_data = ':'.join(chars)
        return id_data


@time_track
def main():
    wb = Workbook()
    ws = wb.active

    fields = ['Кадастровый  номер здания', 'Кадастровый номер (земельного участка)', 'по документу']
    numbers = one_number_get(xlsx_data_parsing())  # activate generator numbers in data_numbers list

    parser = [Parser(number) for number in numbers]

    pool = ThreadPool(50)

    pool.map(lambda f: f.run(), parser)

    ws.append(fields)
    for page in parser:
        for data in page.data_for_record:
            ws.append(data)
    wb.save('result2_1.xlsx')


# ThreadPool(101) - Time run func 10267 sec. 4993 numbers
# ThreadPool(150) - Time run func 14316 sec. 4999 numbers
# ThreadPool(50) - Time run func 4290 sec. 4100 numbers
# ThreadPool(70) - Time run func 3715 sec. 3918 numbers
# ThreadPool(60) - Time run func 4619 sec. 4382 numbers
if __name__ == '__main__':
    main()
