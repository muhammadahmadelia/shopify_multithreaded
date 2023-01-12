
import json
import requests
import datetime
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from models.store import Store
from models.brand import Brand
from models.product import Product
from models.metafields import Metafields
from models.variant import Variant

import threading

class myScrapingThread(threading.Thread):
    def __init__(self, threadID: int, name: str, obj, brand: Brand, glasses_type: str, product_url: str, page_url: str) -> None:
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.brand = brand
        self.glasses_type = glasses_type
        self.product_url = product_url
        self.page_url = page_url
        self.obj = obj
        self.status = 'in progress'
        pass

    def run(self):
        self.obj.get_product_variants_data(self.brand, self.glasses_type, self.product_url, self.page_url)
        self.status = 'completed'

    def active_threads(self):
        return threading.activeCount()


class Rudyproject_Scraper:
    def __init__(self, DEBUG: bool, result_filename: str, logs_filename : str) -> None:
        self.DEBUG = DEBUG
        self.result_filename = result_filename
        self.logs_filename = logs_filename
        self.logs = []
        self.thread_list = []
        self.thread_counter = 0
        self.data = []
        pass
    
    def controller(self, store: Store, brands_with_types: list[dict]) -> None:
        try:
            for brand_with_type in brands_with_types:
                brand: Brand = brand_with_type['brand']
                print(f'Brand: {brand.name}')

                for glasses_type in brand_with_type['types']:
                    print(f'Scraping Type: {glasses_type}')

                    scraped_products = 0
                    total_products = 0
                    start_time = datetime.now()
                    print(f'Start Time: {start_time.strftime("%A, %d %b %Y %I:%M:%S %p")}')

                    for url in self.get_urls_for_type(glasses_type):
                        page_url = url
                        total_products = 0
                        while True:
                            response = self.make_request(page_url, store.link)

                            if response and response.status_code == 200:
                                products_data = self.get_products_data(response)
                                total_products += len(products_data)

                                for product_name, product_link in products_data:
                                    scraped_products += 1
                                    # self.get_product_variants_data(brand, glasses_type, product_link, page_url)
                                    self.create_thread(brand, glasses_type, product_link, page_url)

                                next_page_url = self.get_next_page_url(response.text)
                                if next_page_url: page_url = next_page_url
                                else: break

                            else: self.print_logs(f'Failed to get url {url}')

                        self.wait_for_thread_list_to_complete()
                        self.save_to_json(self.data)
                    
                    end_time = datetime.now()
                    print(f'End Time: {end_time.strftime("%A, %d %b %Y %I:%M:%S %p")}')
                    print(f'Total products scraped: {scraped_products}')                    
                    print('Duration: {}\n'.format(end_time - start_time))
        except Exception as e:
            self.print_logs(f'Exception in Rudyproject_Scraper controller: {e}')
            if self.DEBUG: print(f'Exception in Rudyproject_Scraper controller: {e}')
        finally:
            self.wait_for_thread_list_to_complete()
            self.save_to_json(self.data)

    def get_urls_for_type(self, glasses_type: str) -> list[str]:
        urls = []
        try:
            if glasses_type == 'Sunglasses':
                urls = ['https://www.rudyproject.com/it/en/products/performance-eyewear.html', 'https://www.rudyproject.com/it/en/products/active-lifestyle-eyewear.html']
                glasses_type = 'Sunglasses'
            elif glasses_type == 'Ski & Snowboard Goggles':
                urls = ['https://www.rudyproject.com/it/en/products/snow-mx.html']
                glasses_type = 'Ski & Snowboard Goggles'
            elif glasses_type == 'Eyeglasses':
                urls = ['https://www.rudyproject.com/it/en/products/rxoptical/models.html']
                glasses_type = 'Eyeglasses'
        except Exception as e:
            self.print_logs(f'Exception in get_urls_for_type: {e}')
            if self.DEBUG: print(f'Exception in get_urls_for_type: {e}')
        finally: return urls
    
    def make_request(self, url: str, referer_url: str):
        response = ''
        try:
            headers = self.get_headers(referer_url)
            for _ in range(0, 10):
                try:
                    response = requests.get(url=url, headers=headers)
                    break
                except: sleep(0.1)
        except Exception as e:
            self.print_logs(f'Exception in make_request: {e}')
            if self.DEBUG: print(f'Exception in make_request: {e}')
        finally: return response

    def get_headers(self, referer_url: str) -> dict:
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'referer': referer_url,
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1' }

    def get_products_data(self, response) -> list[str]:
        products = []
        try:
            soup = BeautifulSoup(response.text, 'lxml')
            for div in soup.select('div#ctl00_CPH_Content_Panel_List > div'):
                link = str(div.select_one('a[id$="Link_Detail"]').get('href'))
                if 'code=' in link:
                    product_name = str(div.find('p', {'class': 'box-prodotto__title title--xsmall title'}).text).strip()
                    product_link = F'https://www.rudyproject.com{link}'
                    products.append([product_name, product_link])
                else:
                    response_2 = self.make_request(f'https://www.rudyproject.com{link}', 'https://www.rudyproject.com')
                    if response_2.status_code == 200:
                        soup_2 = BeautifulSoup(response_2.text, 'lxml')
                        for div in soup_2.select('div#ctl00_CPH_Content_Panel_List > div'):
                            link = str(div.select_one('a[id$="Link_Detail"]').get('href'))
                            if 'code=' in link:
                                product_name = str(div.find('p', {'class': 'box-prodotto__title title--xsmall title'}).text).strip()
                                product_link = F'https://www.rudyproject.com{link}'
                                products.append([product_name, product_link])
        except Exception as e:
            self.print_logs(f'Exception in get_products_data: {str(e)}')
            if self.DEBUG: print(f'Exception in get_products_data: {str(e)}')
            else: pass
        finally: return products

    def get_next_page_url(self, response_text: str) -> str:
        next_page_url = ''
        try:
            soup = BeautifulSoup(response_text, 'lxml')
            value = soup.select('div.pagination > ul > li')[-2].find('a').get('href')
            if value: next_page_url = 'https://www.rudyproject.com' + str(value)
        except Exception as e:
            self.print_logs(f'Exception in get_next_page_url: {e}')
            if self.DEBUG: print(f'Exception in get_next_page_url: {e}')
        finally: return next_page_url

    def get_product_variations(self, soup, metafields: Metafields):
        sku, price, availablity, img_url = '', '', '', ''
        try:
            try:
                sku = str(soup.select_one('div[class$="align-center-small mb-small-2"]').text).strip()
            except Exception as e:
                self.print_logs(f'Exception in getting variant sku: {str(e)}')
                if self.DEBUG: print(f'Exception in getting variant sku: {str(e)}')
                else: pass

            try:
                price = str(soup.select_one('div[class="price"]').text).replace('€', '').replace(',', '.').strip() if soup.select_one('div[class="price"]') else ''
            except Exception as e:
                self.print_logs(f'Exception in getting variant price: {str(e)}')
                if self.DEBUG: print(f'Exception in getting variant price: {str(e)}')
                else: pass

            try:
                availability_btn = soup.select_one('a[id$="Button_Cart"]')
                if availability_btn: availablity = 'Active'
                else: availablity = 'Draft'
            except Exception as e:
                self.print_logs(f'Exception in getting availability: {str(e)}')
                if self.DEBUG: print(f'Exception in getting availability: {str(e)}')
                else: pass
                availablity = 'Draft'

            try:
                img_url = 'https://www.rudyproject.com' + str(soup.select_one('a[data-fancybox="images"]').get('href'))
            except Exception as e:
                self.print_logs(f'Exception in getting variant img_url: {str(e)}')
                if self.DEBUG: print(f'Exception in getting variant img_url: {str(e)}')
                else: pass

            try:
                for a_tag in soup.select('div[class^="product-slider__gallery"] > div[class="swiper-wrapper"] > div > a'):
                    metafields.img_360_urls.append(f'https://www.rudyproject.com{a_tag.get("href")}')
            except Exception as e:
                self.print_logs(f'Exception in getting variant img_360_url: {str(e)}')
                if self.DEBUG: print(f'Exception in getting variant img_360_url: {str(e)}')
                else: pass 

        except Exception as e:
            self.print_logs(f'Exception in get_product_variations: {str(e)}')
            if self.DEBUG: print(f'Exception in get_product_variations: {str(e)}')
            else: pass  
        finally: return sku, price, availablity, img_url

    def save_to_json(self, products: list[Product]) -> None:
        try:
            json_products = []
            
            for product in products:
                json_varinats = []
                for index, variant in enumerate(product.variants):
                    json_varinat = {
                        'position': (index + 1), 
                        'title': variant.title, 
                        'sku': variant.sku, 
                        'inventory_quantity': variant.inventory_quantity,
                        'found_status': variant.found_status, 
                        'price': variant.listing_price, 
                        'barcode_or_gtin': variant.barcode_or_gtin,
                        'size': variant.size,
                        'weight': variant.weight
                    }
                    json_varinats.append(json_varinat)
                json_product = {
                    'brand_id': product.brand_id, 
                    'number': product.number, 
                    'name': product.name, 
                    'frame_code': product.frame_code, 
                    'frame_color': product.frame_color, 
                    'lens_code': product.lens_code, 
                    'lens_color': product.lens_color, 
                    'status': product.status, 
                    'type': product.type, 
                    'url': product.url, 
                    'metafields': [
                        { 'key': 'for_who', 'value': product.metafields.for_who },
                        { 'key': 'product_size', 'value': product.metafields.product_size }, 
                        { 'key': 'lens_material', 'value': product.metafields.lens_material }, 
                        { 'key': 'lens_technology', 'value': product.metafields.lens_technology }, 
                        { 'key': 'frame_material', 'value': product.metafields.frame_material }, 
                        { 'key': 'frame_shape', 'value': product.metafields.frame_shape },
                        { 'key': 'gtin1', 'value': product.metafields.gtin1 }, 
                        { 'key': 'img_url', 'value': product.metafields.img_url },
                        { 'key': 'img_360_urls', 'value': product.metafields.img_360_urls }
                    ],
                    'variants': json_varinats
                }
                json_products.append(json_product)
        
            with open(self.result_filename, 'w') as f: json.dump(json_products, f)
            
        except Exception as e:
            self.print_logs(f'Exception in save_to_json: {e}')
            if self.DEBUG: print(f'Exception in save_to_json: {e}')
            else: pass

    # print logs to the log file
    def print_logs(self, log: str) -> None:
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

    def create_thread(self, brand: Brand, glasses_type: str, product_url: str, page_url: str):
        thread_name = "Thread-"+str(self.thread_counter)
        self.thread_list.append(myScrapingThread(self.thread_counter, thread_name, self, brand, glasses_type, product_url, page_url))
        self.thread_list[self.thread_counter].start()
        self.thread_counter += 1

    def is_thread_list_complted(self):
        for obj in self.thread_list:
            if obj.status == "in progress":
                return False
        return True

    def wait_for_thread_list_to_complete(self):
        while True:
            result = self.is_thread_list_complted()
            if result: 
                self.thread_counter = 0
                self.thread_list.clear()
                break
            else: sleep(1)

    def get_product_variants_data(self, brand: Brand, glasses_type: str, product_url: str, page_url: str) -> None:
        try:
            product_response = self.make_request(product_url, page_url)
            if product_response.status_code == 200:
                soup = BeautifulSoup(product_response.text, 'lxml')

                gender, size = self.get_gender_and_size(soup)
                all_varinat_skus = self.get_all_variants_skus(soup, product_url)

                model_skus = []
                flag = True
                while flag:
                    product = Product()
                    product.brand_id = brand.id
                    product.type = glasses_type
                    product.status = 'active'
                    product.url = product_url

                    metafields = Metafields()
                    metafields.for_who = str(gender).strip().title()
                    metafields.product_size = str(size).strip()

                    try:
                        value = str(soup.select_one('strong[class="text--black"]').text).strip().title()
                        if ' +' in str(value).strip(): value = str(value).replace(' +', '+')
                        product.name = str(value).strip().title()
                        value2 = str(soup.select_one('h1[class^="title"]').text).strip().title()
                        if value2 not in product.name: product.name = f'{value2} {product.name}'

                        # if ' +' in str(value).strip(): value = str(value).replace(' +', '+')
                        # if str(product.name).strip().lower() in str(value).strip().lower():
                        #     value = str(value).lower().replace(str(product.name).lower(), '').strip()
                        
                        if str('Impactx Photochromic 2').strip().lower() in value.lower() or str('Impactx 2').strip().lower() in value.lower(): 
                            metafields.lens_technology = 'Impactx Photochromic 2'
                            split_value = ''
                            if str('Impactx Photochromic 2').strip().lower() in str(value).strip().lower(): split_value = 'Impactx Photochromic 2'
                            elif str('Impactx 2').strip().lower() in str(value).strip().lower(): split_value = 'Impactx 2'
                            if split_value:
                                values = str(value).lower().split(str(split_value).strip().lower())
                                if len(values) == 2:
                                    product.frame_color = str(values[0]).strip().title()
                                    product.lens_color = str(values[1]).strip().title()
                        elif str('Rp Optics').strip().lower() in value.lower(): 
                            metafields.lens_technology = 'Rp Optics'
                            if str(metafields.lens_technology).strip().lower() in str(value).strip().lower():
                                values = str(value).lower().split(str(metafields.lens_technology).strip().lower())
                                if len(values) == 2:
                                    product.frame_color = str(values[0]).strip().title()
                                    product.lens_color = str(values[1]).strip().title()
                        elif str('Pol. 3Fx').strip().lower() in value.lower() or str('Polar 3FX').strip().lower() in value.lower(): 
                            metafields.lens_technology = 'Polarized'
                            split_value = ''
                            if str('Pol. 3Fx').strip().lower() in str(value).strip().lower(): split_value = 'Pol. 3Fx'
                            elif str('Polar 3FX').strip().lower() in str(value).strip().lower(): split_value = 'Polar 3FX'
                            if split_value:
                                values = str(value).lower().split(str(split_value).strip().lower())
                                if len(values) == 2:
                                    product.frame_color = str(values[0]).strip().title()
                                    product.lens_color = str(values[1]).strip().title()
                        else:
                            metafields.lens_technology = 'Rp Optics' 
                            if '-' in value:
                                values = value.split('-')
                                product.frame_color = values[0].strip()
                                product.lens_color = values[1].strip()
                            else: product.frame_color = value

                        if len(product.frame_color) > 0 and str(product.frame_color).strip()[-1] == '-': product.frame_color = product.frame_color[0:-1]
                    
                    except Exception as e:
                        self.print_logs(f'Exception in getting product full name: {str(e)}')
                        if self.DEBUG: print(f'Exception in getting product full name: {str(e)}')
                        else: pass


                    sku, price, availablity, metafields.img_url = self.get_product_variations(soup, metafields)
                    product.number = str(sku).strip().upper()


                    variant = Variant()
                    variant.position = 1
                    if '\u00ac' in metafields.product_size: metafields.product_size = str(metafields.product_size).replace('\u00ac', '-').strip()
                    if metafields.product_size:
                        variant.size = metafields.product_size
                        if '#' in metafields.product_size: 
                            variant.title = str(metafields.product_size).split('#')[0].strip()
                    variant.sku = sku
                    variant.listing_price = price
                    variant.found_status = 1
                    if availablity == 'Active': variant.inventory_quantity = 1
                    else: variant.inventory_quantity = 0

                    if '+' in product.frame_color:
                        product.frame_color = str(product.frame_color).split('+')[-1].strip()

                    product.metafields = metafields
                    product.variants = variant

                    model_skus.append(sku)

                    self.data.append(product)
                    flag = False

                    for variation_sku in all_varinat_skus:
                        if variation_sku not in model_skus:
                            next_variant_url = f"{str(product_url).split('=')[0]}={variation_sku}"
                            product_url = next_variant_url
                            product_response = self.make_request(next_variant_url, product_url)
                            if product_response.status_code == 200:
                                soup = BeautifulSoup(product_response.text, 'lxml')
                                flag = True
                                break

            else: self.print_logs(f'Failed to get url {product_url}')
        except Exception as e:
            self.print_logs(f'Exception in get_product_variants_data: {e}')
            if self.DEBUG: print(f'Exception in get_product_variants_data: {e}') 

    def get_gender_and_size(self, soup: BeautifulSoup):
        gender, size = '', ''
        try:
            for li in soup.select('ul[class="product-detail__list"] > li'):
                label = li.select_one('span[class="product-detail__list-label"]').text
                value = li.select_one('span[class="product-detail__list-content"]').text
                if str(label).strip().lower() == 'gender':
                    if str(value).strip().lower() == 'w': gender = 'WOMEN'
                    else: gender = value
                elif str(label).strip().lower() == 'dimension': size = value
        except Exception as e:
            self.print_logs(f'Exception in get_gender_and_size: {str(e)}')
            if self.DEBUG: print(f'Exception in get_gender_and_size: {str(e)}')
            else: pass
        finally: return gender, size

    def get_all_variants_skus(self, soup: BeautifulSoup, product_url: str) -> list[str]:
        all_varinat_skus = []
        try:
            divs = soup.select('div[class="button-select__item"]')
            if len(divs) > 0:
                for div in soup.select('div[class="button-select__item"]'):
                    variation_sku = div.get('variant-code')
                    if variation_sku not in all_varinat_skus: all_varinat_skus.append(variation_sku)
                variation_sku = str(product_url).split('=')[-1].strip()
                if variation_sku not in all_varinat_skus: all_varinat_skus.append(variation_sku)
            else: 
                variation_sku = str(product_url).split('=')[-1].strip()
                if variation_sku not in all_varinat_skus: all_varinat_skus.append(variation_sku)
        except Exception as e:
            self.print_logs(f'Exception in get_gender_and_size: {str(e)}')
            if self.DEBUG: print(f'Exception in get_gender_and_size: {str(e)}')
            else: pass
        finally: return all_varinat_skus

    def printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total: 
            print()