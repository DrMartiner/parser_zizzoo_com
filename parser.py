#!/usr/bin/env python

import sys
from collections import OrderedDict
from datetime import datetime
import json
import logging
from os import path, makedirs
import re

from grab.spider import Spider, Task
from lxml.html import fromstring
from selection import XpathSelector
from weblib.error import DataNotFound

from config.config_parser import ConfigReader


# Set absolute path of working directory
ROOT_DIR = path.dirname(path.abspath(__file__))
LOG_FORMAT = "'%(filename)s[LINE:%(lineno)d]#%(levelname)-8s [%(asctime)s] \
%(message)s'"


class MySpider(Spider):
    initial_urls = ['https://www.zizoo.com/en/b/']
    type_file = 'json'

    def __init__(self, *args, **kwargs):
        self._ext_config = kwargs.pop('ext_config')
        super(self, *args, **kwargs).__init__(*args, **kwargs)

    def prepare(self):
        """Must be done before begin parsing"""
        self.parsed_urls = []
        if self._ext_config['initialurl']:
            self.initial_urls = [self._ext_config['initialurl']]

    def get_id(self, url):
        """Get unique slug page"""
        return url.split('/')[-1]

    def save_result(self, file_id, data):
        """Save received data into json-format"""
        filename = path.join(
            self._ext_config['dirresults'],
            "{0}.{1}".format(file_id, self.type_file)
        )
        with open(filename, 'w') as file:
            file.write(data)
        logging.info("File %s has beed saved!" % filename)

    def file_exist(self, file_id):
        """Check on file exists with that name"""
        filename = path.join(
            self._ext_config['dirresults'],
            "{0}.{1}".format(file_id, self.type_file)
        )
        if path.exists(filename):
            return True
        else:
            return False

    def task_initial(self, grab, task):
        """Begining parsing"""
        for elem in grab.doc.select('//a[@class="boat__figure"]'):
            yield Task('item', elem.attr('href'), page=grab.doc.url)

        url_page_next = grab.doc.select('//a[@title="Next"]')
        if url_page_next.exists():
            yield Task(
                'page',
                url=grab.make_url_absolute(url_page_next.attr('href'))
            )
        else:
            logging.debug("Next page doesn't exist. Current url %s: "
                          % grab.doc.url)

    def task_page(self, grab, task):
        """Parsing pages. https://www.zizoo.com/en/b/?page={NUM}"""
        logging.debug("Parsing current page: %s" % grab.doc.url)
        for elem in grab.doc.select('//a[@class="boat__figure"]'):
            if not self.file_exist(self.get_id(elem.attr('href'))) \
                    or config['rewrite_files']:
                yield Task('item', elem.attr('href'), page=grab.doc.url)
            else:
                logging.info("Item will not parse since file exists: %s.%s"
                             % (self.get_id(elem.attr('href')), self.type_file)
                             )

        # Search next link
        url_page_next = grab.doc.select('//a[@title="Next"]')
        if url_page_next.exists():
            # If next link exists then task add
            yield Task(
                'page',
                url=grab.make_url_absolute(url_page_next.attr('href'))
            )
        else:
            logging.debug("Next page doesn't exist. Current url: %s"
                          % grab.doc.url)

    def task_item(self, grab, task):
        """Parsing information about boat"""
        if self.file_exist(self.get_id(grab.doc.url)) \
                and not config['rewrite_files']:
            logging.info("Item will not parse since file exists: %s.%s page:%s"
                         % (self.get_id(grab.doc.url),
                            self.type_file,
                            task.page)
                         )
            return

        logging.debug("Begining item parsing: %s" % grab.doc.url)
        json_info = {}
        realtime_found = None
        try:
            realtime_found = grab.doc.rex_text("'boatBanner'\s*:\s*'(.*?)',")
        except DataNotFound:
            logging.warning(
                "Repeat... 'boatBanner' for realtimeavibility not found in: %s"
                % grab.doc.url
            )
            # Repeat task
            yield Task('item', url=grab.config['url'],
                       task_try_count=task.task_try_count + 1)

        data_boat = grab.doc.select('//span[@class="wishlist-btn ' +
                                    'js-wishlist-toggle boatview__wishlist"]')
        try:
            json_info = json.loads(data_boat.attr('data-boat'))
        except json.decoder.JSONDecodeError:
            logging.warning("Json decode error for data-boat in: %s"
                            % grab.doc.url)
            # Repeat task
            yield Task('item', url=grab.config['url'],
                       task_try_count=task.task_try_count + 1)
        except IndexError:
            logging.warning("span js-wishlist-toggle... not found in: %s"
                            % grab.doc.url)
            # Repeat task
            yield Task('item', url=grab.config['url'],
                       task_try_count=task.task_try_count + 1)

        if len(json_info) < 1 or realtime_found is None:
            return

        realtime = True if realtime_found == 'realtime' else False
        info = OrderedDict()
        info['url'] = grab.doc.url
        info['title'] = self.get_title(grab)
        info['parsingdate'] = datetime.now().strftime('%H:%M %d/%m/%y')
        info['realtimeavilbility'] = realtime

        location = json_info['location']
        info['location'] = OrderedDict([
            ('country', location.split(', ')[0]),
            ('city', location.split(', ')[1])
        ])

        data = OrderedDict(info)

        data['year'] = self.get_year(grab)
        data['length'] = json_info['length'].replace(' ', '')

        guests = self.get_guests(grab, json_info)
        if guests is not None:
            data['guests'] = int(guests)
        data['type'] = grab.doc.rex_text("'type': '(.+?)',")

        engine_value = self.get_engine(grab)
        if engine_value is not None:
            data['engine'] = engine_value

        sleeps = self.get_sleeps(grab)
        if sleeps is not None:
            data['sleeps'] = sleeps

        cabins = self.get_cabins(grab, json_info)
        if cabins is not None:
            data['cabins'] = cabins

        bathrooms = self.find_boatview__stats(grab, 'Bathrooms')
        if bathrooms is not None:
            data['bathrooms'] = int(bathrooms)
        else:
            logging.debug("Bathrooms for 'bathrooms' not found in: %s"
                          % grab.doc.url)

        about = self.get_about(grab)
        if about is None:
            logging.debug("About for 'about' not found in: %s"
                          % grab.doc.url)
        data['about'] = about if about is not None else ''
        data['photos'] = self.get_images_urls(grab)

        inventory = self.get_inventory(grab)
        if inventory is not None:
            data['inventory'] = inventory

        data['pickup'] = self.get_pickup(grab)

        equipment = self.get_equipment(grab)
        if len(equipment) < 1:
            logging.debug("equipment not found in: %s"
                          % grab.doc.url)
        else:
            data['equipment'] = equipment

        prices = self.get_prices(grab, 'Obligatory extras')
        optional = self.get_prices(grab, 'Optional extras')
        if prices is not None:
            data['prices'] = OrderedDict([
                ('obligatory', prices),
            ])
        if optional is not None:
            data['optional'] = optional

        if self.file_exist(self.get_id(grab.doc.url)) \
                and not config['rewrite_files']:
            logging.info("Item will not save since file exists: %s.%s"
                         % (self.get_id(grab.doc.url), self.type_file)
                         )
            return

        # If elements more than 10 then save results into json-format
        if len(data) > 9:
            logging.debug("Saving url: %s from page: %s"
                          % (grab.doc.url, task.page))
            self.save_result(
                self.get_id(grab.doc.url),
                json.dumps(data, ensure_ascii=False, indent=2)
            )
        else:
            logging.info(
                "Data hasn't been saved. It contains less 10 objects: %s.%s"
                % (self.get_id(grab.doc.url), self.type_file)
            )
            # Repeat task
            yield Task('item', url=grab.config['url'],
                       task_try_count=task.task_try_count + 1)

    def get_prices(self, grab, subject):
        """Parsing information about Obligatory extras and Optional extras
        for objects are prices and optional"""
        prices = []
        try:
            extras = grab.doc.rex_text(
                '<h3 class\="h6 copy-sp-m">.*?%s.*?</h3>(.+?)</ul>' % subject,
                flags=re.S
            )
        except DataNotFound:
            logging.debug(
                "Price %s is not found on %s"
                % (subject, grab.doc.url)
            )
            return None

        sel = XpathSelector(fromstring(extras))
        prices = []
        for li in sel.select('//li[@class="list__item u-cf"]'):
            obligatory = OrderedDict()
            obligatory['name'] = li.select('node()').text()
            money = li.select('node()/strong').text()
            obligatory['value'] = money[1:].replace(',', '')

            # Find perweek or perday
            if li.select(
                'span[@class="boatview__extras-amount"' +
                ' and contains(text(),"per week")]'
            ).exists():
                obligatory['perweek'] = True
            elif li.select(
                'span[@class="boatview__extras-amount"' +
                ' and contains(text(),"per day")]'
            ).exists():
                obligatory['perday'] = True
            obligatory['currency'] = money[0]
            prices.append(obligatory)

        if len(prices) < 1:
            logging.debug(
                "Price %s contains less than one element on: %s"
                % (subject, grab.doc.url)
            )
            return None

        return prices

    def get_equipment(self, grab):
        """Parsing equipment"""
        equipment = OrderedDict()
        grid = grab.doc.select(
            '//h2[@id="equipment"]'
        )
        for item in grid.select(
            '//div[@class="grid__unit"]'
        ).select(
                'div[@class="h6 copy-sp-m"]'):
            ul = item.select('//ul[@class="list-bulleted"]')
            equipment[item.text()] = [
                value.text() for value in ul.select('li[@class="list__item"]')
            ]

        return equipment

    def get_pickup(self, grab):
        """Parsing pickup"""
        pickup = OrderedDict()

        elements = self.find_in_card__body(
            grab,
            '/p[@class="p--s copy-sp-m"]',
        )

        checkin, checkout = elements.replace(
            "Check-in: ", ""
        ).split(
            ' Check-out: '
        )

        pickup['checkin'] = OrderedDict([
            ('datetime', checkin.split(", ")[0]),
            ('location', checkin.split(", ")[1])
        ])

        pickup['checkout'] = OrderedDict([
            ('datetime', checkout.split(", ")[0]),
            ('location', checkout.split(", ")[1])
        ])

        return pickup

    def get_inventory(self, grab):
        """Parsing inventory"""
        ul = grab.doc.select('//ul[@class="boatview__equipment-list"]')

        if ul.exists():
            return [
                value.text()
                for value in ul[0].select('li[@class="list__item"]')
            ]
        else:
            logging.debug("boatview__equipment-list for 'inventory'" +
                          " not found in: %s" % grab.doc.url)
            return None

    def get_images_urls(self, grab, parse_first_image=True):
        """Parsing urls of images for photos"""
        images = []
        if parse_first_image:
            first_image = grab.doc.select(
                '//figure[@class="item"]' +
                '/img[@class="img-fluid"]'
            )
            if first_image.exists() and 'http' in first_image.attr('src'):
                images.append(first_image.attr('src'))

        for image in grab.doc.select(
                '//figure[@class="item"]' +
                '/img[@class="lazyOwl img-fluid"]'):
            images.append(image.attr('data-src'))
        if len(images) < 1:
            logging.debug("Images not found in: %s" % grab.doc.url)

        return images

    def get_about(self, grab):
        """Parsing article for about"""
        about = self.find_in_card__body(
            grab,
            '//div[@class="boatview__description"]'
        )
        if about is not None:
            return about
        else:
            about = self.find_in_card__body(grab, '//p')
            if about is None or len(about) < 90:
                return None
        return about

    def find_in_card__body(self, grab, pattern, normalize_space=True):
        """Search elements in class: card__body card__body--l"""
        value = grab.doc.select(
            '//div[@class="card__body card__body--l"]' +
            pattern
        )

        if value.exists():
            return value.text(normalize_space=normalize_space)
        else:
            return None

    def get_cabins(self, grab, json_info):
        """Parsing cabins"""
        cabins = json_info.get('cabins')
        if cabins is not None:
            return int(cabins)
        cabins = self.find_boatview__stats(grab, "Cabins")
        if cabins is not None:
            return int(cabins)
        else:
            cabins = self.find_boatview__stats(grab, "Double cabins")
            if cabins is not None:
                return int(cabins)
            else:
                logging.debug("Cabins not found in: %s" % grab.doc.url)
                return None

    def get_sleeps(self, grab):
        """Parsing sleeps"""
        sleeps = self.find_boatview__stats(grab, 'Sleeps')
        if sleeps is not None:
            return sleeps

    def get_engine(self, grab):
        """Parsing for engine object"""
        engine_value = self.find_boatview__stats(grab, 'Sail type')
        if engine_value is not None:
            return engine_value
        else:
            logging.debug("Sail type for 'engine' not found in: %s"
                          % grab.doc.url)
            engine_value = self.find_boatview__stats(grab, 'Engine')
            if engine_value is not None:
                return engine_value
            else:
                logging.debug("Engine for 'engine' not found in: %s"
                              % grab.doc.url)

    def get_guests(self, grab, json_info):
        """Parsing for guests object"""
        guests = json_info.get('guests')
        if guests is not None:
            return int(guests)
        guests = self.find_boatview__stats(grab, "Max. guests")
        if guests is not None:
            return int(guests)
        else:
            logging.debug("Guests not found in: %s" % grab.doc.url)
            return None

    def find_boatview__stats(self, grab, pattern):
        """Find elements in class: boatview__stats-label
        and inside contains determined text"""
        value = grab.doc.select(
            '//span[contains(@class, "boatview__stats-label")' +
            ' and contains(text(),"%s")]' % pattern +
            '/../span[@class="boatview__stats-value"]'
        )

        if value.exists():
            return value.text()
        else:
            return None

    def get_year(self, grab):
        """Parsing boat year old"""
        return int(
            grab.doc.select(
                '//time[@itemprop="releaseDate"]'
            ).attr('datetime')
        )

    def get_title(self, grab):
        """Parsing title"""
        return grab.doc.select('//h1[@class="h2 copy-sp-s"]').text()


if __name__ == '__main__':
    cls = globals()[sys.argv[1] if len(sys.argv) > 1 else 'MySpider']

    # Integer values of debug constants
    LEVELS_DEBUG = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0
    }

    # Load config from file and set determined type
    config_obj = ConfigReader(file_config='settings.ini')
    config = config_obj.config_read(types_params={
        'useproxy': 'bool',
        'uselog': 'bool',
        'logintofile': 'bool',
        'numthreads': 'int',
        'network_try_limit': 'int',
        'periodproxyupdate': 'int',
        'rewrite_files': 'bool'
    })

    # Section for logging
    if config['uselog']:
        prefix_date = "{0}-{1}-{2}_{3}-{4}".format(
            datetime.now().day,
            datetime.now().month,
            datetime.now().year,
            datetime.now().hour,
            datetime.now().minute
        )
        level = LEVELS_DEBUG.get(config['level'], 0)
        if config['logintofile'] and '/' not in config['logfile'] \
                and '\\' not in config['logfile']:
            config['logfile'] = path.join(
                ROOT_DIR,
                'logs',
                '%s_%s' % (prefix_date, config['logfile'])
            )

        if config['logintofile']:
            if not path.isdir(config['dirresults']):
                makedirs(path.dirname(path.abspath(config['logfile'])))
            filename = config['logfile']
        else:
            filename = None

        logging.basicConfig(
            level=logging.getLevelName(level),
            filename=filename,
            format=LOG_FORMAT
        )

    # If directory for results are saving not contains path's directory
    # (in other words it's relative)
    # then add full path for directory
    if '/' not in config['dirresults'] and '\\' not in config['dirresults']:
        config['dirresults'] = path.join(ROOT_DIR, config['dirresults'])
    if not path.isdir(config['dirresults']):
        makedirs(config['dirresults'])

    bot = cls(
        thread_number=config['numthreads'],
        network_try_limit=config['network_try_limit'],
        ext_config=config
    )

    # bot.setup_cache(database='zizoo_net')
    # bot.setup_queue(backend='mongo', database='zizoo_tasks')

    # Section for determine list of proxies and type proxy
    if config['useproxy']:
        if config['listproxies'].startswith('http://') \
                or config['listproxies'].startswith('https://'):
            source_type = 'url'
        else:
            source_type = 'text_file'

        bot.load_proxylist(
            config['listproxies'],
            source_type=source_type,
            proxy_type=config['typeproxy'],
            auto_change=True
        )

    # Start parser
    try:
        bot.run()
    except KeyboardInterrupt:
        pass

    # Show statistic work at the end
    logging.info(bot.render_stats())
