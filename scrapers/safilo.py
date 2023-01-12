
import json
import threading
import datetime
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from models.store import Store
from models.brand import Brand
from models.product import Product
from models.metafields import Metafields
from models.variant import Variant



class Safilo_Scraper:
    def __init__(self, DEBUG: bool, result_filename: str, logs_filename: str) -> None:
        self.DEBUG = DEBUG
        self.data = []
        self.result_filename = result_filename
        self.logs_filename = logs_filename
        self.thread_list = []
        self.thread_counter = 0
        self.chrome_options = Options()
        self.chrome_options.add_argument('--disable-infobars')
        self.chrome_options.add_argument("--start-maximized")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.args = ["hide_console", ]
        self.browser = webdriver.Chrome(options=self.chrome_options, service_args=self.args)
        pass

    def controller(self, store: Store, brands_with_types: list[dict]) -> None:
        try:
            self.browser.get(store.link)
            self.wait_until_browsing()
            self.accept_cookies()
            
            if self.login(store.username, store.password):
                self.wait_for_page_loading()

                self.select_language()
                sleep(0.8)
                self.wait_for_page_loading()


                for brand_with_type in brands_with_types:
                    brand: Brand = brand_with_type['brand']
                    print(f'Brand: {brand.name}')

                    for glasses_type in brand_with_type['types']:

                        if self.select_brand(brand.name):
                            self.select_sunglasses_category(glasses_type)
                            self.load_all_products()
                        
                            self.wait_until_element_found(40, 'xpath', '//div[@class="productListContent cc_results_list cc_grid_container"]/span[@class="cc_product_container productFlexItem"]')

                            total_products = self.get_total_products()
                            scraped_products = 0

                            print(f'Type: {glasses_type} | Total products: {total_products}')
                            start_time = datetime.now()
                            print(f'Start Time: {start_time.strftime("%A, %d %b %Y %I:%M:%S %p")}')

                            self.printProgressBar(scraped_products, total_products, prefix = 'Progress:', suffix = 'Complete', length = 50)

                            for product_span in self.browser.find_elements(By.XPATH, '//div[@class="productListContent cc_results_list cc_grid_container"]/span[@class="cc_product_container productFlexItem"]'):
                                scraped_products += 1
                                ActionChains(self.browser).move_to_element(product_span).perform()
                                
                                product_url, product_name, product_number = self.get_product_data(product_span, brand)
                                                            
                                self.open_new_tab(product_url)
                                self.wait_for_page_loading()
                                
                                for_who, frame_material, template_material, type_material, frame_shape = self.get_main_information()
                                if not frame_material: frame_material = template_material 
                                
                                # if self.DEBUG: print(product_number, product_name)

                                for index, div in enumerate(self.browser.find_elements(By.XPATH, '//div[@class="productVariant row"]')):
                                    try:
                                        product = Product()
                                        product.brand_id = brand.id
                                        product.number = str(product_number).strip().upper()
                                        product.name = str(product_name).strip().upper()
                                        self.clean_product_name(product)
                                        product.url = product_url
                                        product.type = glasses_type
                                        product.status = 'active'

                                        ActionChains(self.browser).move_to_element(div).perform()
                                        metafileds = Metafields()
                                        metafileds.for_who = str(for_who).strip().title()
                                        metafileds.frame_material = str(frame_material).strip().title()
                                        metafileds.frame_shape = str(frame_shape).strip().title()

                                        try: metafileds.img_url = div.find_element(By.CSS_SELECTOR, 'div[class$="model-info-section"]').find_element(By.TAG_NAME, 'img').get_attribute('src')
                                        except Exception as e:
                                            self.print_logs(f'Exception in getting variant image url: {str(e)}')
                                            if self.DEBUG: print(f'Exception in getting variant image url: {str(e)}')
                                            else: pass             
                                        
                                        try:
                                            value = str(div.find_element(By.XPATH, './/div[@class="product_value"]').text).strip()
                                            if '-' in value:
                                                product.frame_code = str(value).split('-')[0].strip().upper()
                                                product.frame_color = str(value).split('-')[-1].strip().title()
                                        except Exception as e:
                                            self.print_logs(f'Exception in getting variant frame code and color: {str(e)}')
                                            if self.DEBUG: print(f'Exception in getting variant frame code and color: {str(e)}')
                                            else: pass

                                        try:
                                            soup = BeautifulSoup(self.browser.page_source, 'lxml')
                                            new_div = soup.find_all('div', {'class': 'productVariant row'})[index]
                                            
                                            labels = new_div.find_all('div', {'class': 'product_label'})
                                            values = new_div.find_all('div', {'class': 'product_value'})

                                            for i in range(0, len(values)):
                                                try:
                                                    label = str(labels[i].text).strip()
                                                    value = str(values[i].text).strip()
                                                    if str('Not defined').strip().lower() in str(value).lower(): value = ''
                                                    if str('Lens Color').strip().lower() == str(label).strip().lower(): 
                                                        if '-' in value:
                                                            product.lens_code = str(value.split('-')[0]).strip().upper()
                                                            product.lens_color = str(value.split('-')[-1]).strip().title()
                                                        else:
                                                            if len(value) == 2: product.lens_code = str(value).strip().upper()
                                                            else: product.lens_color = str(value).strip().title()
                                                    elif str('Lens Material').strip().lower() == str(label).strip().lower(): metafileds.lens_material = str(value).strip().title()
                                                except: pass
                                        except Exception as e:
                                            self.print_logs(f'Exception in getting variant lens color: {str(e)}')
                                            if self.DEBUG: print(f'Exception in getting variant lens color: {str(e)}')
                                            else: pass

                                        try:
                                            for main_div_index, main_div in enumerate(div.find_elements(By.XPATH, './/div[@class="col-md-4 col-sm-6 col-xs-12 blue-bg variant-border"]')):
                                                variant = Variant()
                                                variant.position = (main_div_index + 1)
                                                for inner_div in main_div.find_elements(By.CSS_SELECTOR, 'div[class$="blue-bg variant-border"] > div > div'):
                                                    label = str(inner_div.find_element(By.XPATH, './/div[@class="product_label"]').text).strip()
                                                    value = str(inner_div.find_element(By.XPATH, './/div[@class="product_value bold"]').text).strip()
                                                    
                                                    inner_div_class = str(inner_div.get_attribute('class')).strip()
                                                    if 'field' in inner_div_class:
                                                        if str('Size').strip().lower() in str(label).strip().lower(): 
                                                            variant.title = str(value).split('-')[0].strip()
                                                            variant.size = str(value).strip()
                                                    elif 'variant-sku' in inner_div_class: 
                                                        if 'SKU' in label: variant.sku = value
                                                    elif 'variant-code' in inner_div_class:
                                                        if 'EAN' in label: variant.barcode_or_gtin = value
                                                variant.found_status = 1
                                                avail_value = str(div.find_element(By.CSS_SELECTOR, 'span[class^="product_value"] > span').text).strip()
                                                if str('Out of production').strip().lower() in str(avail_value).strip().lower(): variant.inventory_quantity = 0
                                                elif str('Fast shipping').strip().lower() in str(avail_value).strip().lower(): variant.inventory_quantity = 1
                                                elif str('Restocking').strip().lower() in str(avail_value).strip().lower(): variant.inventory_quantity = 0
                                                elif str('Product Unavailable').strip().lower() in str(avail_value).strip().lower(): variant.inventory_quantity = 1

                                                price_value = str(div.find_element(By.CSS_SELECTOR, "div[class*='product_retail pull-right hidden-xs']").text).strip()
                                                variant.price = price_value.split(' ')[-1].replace(',', '.')
                                                
                                                product.variants = variant
                                        except Exception as e:
                                            self.print_logs(f'Exception in getting variant size, sku, ean: {str(e)}')
                                            if self.DEBUG: print(f'Exception in getting variant size, sku, ean: {str(e)}')
                                            else: pass

                                        for _ in range(0, 10):
                                            try:
                                                for img_tag in self.browser.find_elements(By.CSS_SELECTOR, 'img[class="img-responsive center-block product_img"]'):
                                                    img_url = img_tag.get_attribute('src')
                                                    if f'modelCode={product.number}&colorCode={product.frame_code}&lensCode={product.lens_code}' in img_url:
                                                        if img_url not in metafileds.img_360_urls:
                                                            metafileds.img_360_urls.append(img_tag.get_attribute('src'))
                                                break
                                            except Exception as e:
                                                self.print_logs(f'Exception in getting img_360_urls: {str(e)}')
                                                # if self.DEBUG: print(f'Exception in getting image_360_urls: {str(e)}')
                                                # else: pass
                                                sleep(0.5)

                                        for variant in product.variants:
                                            if str(variant.size).strip():
                                                metafileds.product_size += f'{variant.size}, '
                                            if str(variant.barcode_or_gtin).strip():
                                                metafileds.gtin1 += f'{variant.barcode_or_gtin}, '
                                        
                                        metafileds.product_size = str(metafileds.product_size).strip()
                                        metafileds.gtin1 = str(metafileds.gtin1).strip()

                                        if metafileds.product_size[-1] == ',': metafileds.product_size = metafileds.product_size[:-1]
                                        if metafileds.gtin1[-1] == ',': metafileds.gtin1 = metafileds.gtin1[:-1]
                                        
                                        product.metafields = metafileds

                                        self.data.append(product)
                                        self.save_to_json(self.data)
                                        
                                    except: pass
                                
                                self.printProgressBar(scraped_products, total_products, prefix = 'Progress:', suffix = 'Complete', length = 50)       
                                
                                self.close_last_tab()
                            
                            end_time = datetime.now()
                            print(f'End Time: {end_time.strftime("%A, %d %b %Y %I:%M:%S %p")}')
                            print('Duration: {}\n'.format(end_time - start_time))
                
            else: print(f'Failed to login \nURL: {self.URL}\nUsername: {str(store.username)}\nPassword: {str(store.password)}')
        except Exception as e:
            self.print_logs(f'Exception in Safilo_All_Scraper controller: {e}')
            if self.DEBUG: print(f'Exception in Safilo_All_Scraper controller: {e}')
            else: pass
        finally: self.browser.quit()

    def wait_until_browsing(self) -> None:
        while True:
            try:
                state = self.browser.execute_script('return document.readyState; ')
                if 'complete' == state: break
                else: sleep(0.2)
            except: pass

    def wait_until_element_found(self, wait_value: int, type: str, value: str) -> bool:
        flag = False
        try:
            if type == 'id':
                WebDriverWait(self.browser, wait_value).until(EC.presence_of_element_located((By.ID, value)))
                flag = True
            elif type == 'xpath':
                WebDriverWait(self.browser, wait_value).until(EC.presence_of_element_located((By.XPATH, value)))
                flag = True
            elif type == 'css_selector':
                WebDriverWait(self.browser, wait_value).until(EC.presence_of_element_located((By.CSS_SELECTOR, value)))
                flag = True
            elif type == 'class_name':
                WebDriverWait(self.browser, wait_value).until(EC.presence_of_element_located((By.CLASS_NAME, value)))
                flag = True
            elif type == 'tag_name':
                WebDriverWait(self.browser, wait_value).until(EC.presence_of_element_located((By.TAG_NAME, value)))
                flag = True
        except: pass
        finally: return flag

    def accept_cookies(self) -> None:
        try:
            # accept cookies if found
            if self.wait_until_element_found(30, 'xpath', '//button[@id="acceptCookiesPolicy"]'):
                self.browser.find_element(By.XPATH,'//button[@id="acceptCookiesPolicy"]').click()
                sleep(0.2)
        except Exception as e:
            self.print_logs(f'Exception in accept_cookies: {str(e)}')
            if self.DEBUG: print(f'Exception in accept_cookies: {str(e)}')
            else: pass

    def login(self, email: str, password: str) -> bool:
        login_flag = False
        try:
            if self.wait_until_element_found(20, 'xpath', '//input[@id="emailField"]'):
                self.browser.find_element(By.XPATH, '//input[@id="emailField"]').send_keys(email)
                sleep(0.2)
                if self.wait_until_element_found(20, 'xpath', '//input[@id="passwordField"]'):
                    self.browser.find_element(By.XPATH, '//input[@id="passwordField"]').send_keys(password)
                    sleep(0.2)
                    self.browser.find_element(By.XPATH, '//input[@id="send2Dsk"]').click()

                    if self.wait_until_element_found(20, 'xpath', '//ul[@data-value="Marchi"]'): login_flag = True
                else: print('Password input not found')
            else: print('Email input not found')
        except Exception as e:
            self.print_logs(f'Exception in login: {str(e)}')
            if self.DEBUG: print(f'Exception in login: {str(e)}')
            else: pass
        finally: return login_flag
    
    def wait_for_page_loading(self):
        self.wait_until_browsing()
        for _ in range(0, 100):
            try:
                self.browser.find_element(By.XPATH, '//div[@id="overlay"]')
                sleep(0.5)
            except: break

    def select_language(self):
        try:
            self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.browser.find_element(By.XPATH, '//a[@class="changeLocale cc_change_locale link"]').click()
            for _ in range(0, 100):
                try:
                    if 'display: none;' not in self.browser.find_element(By.XPATH, '//div[@id="switcherMod"]').get_attribute('style'):
                        select = Select(self.browser.find_element(By.XPATH, '//select[@class="selectpicker localeSelector"]'))
                        select.select_by_value('en_US')
                        self.browser.find_element(By.CSS_SELECTOR, 'input[class="btn btn-primary setLocale cc_set_locale"]').click()
                        sleep(1)
                        break
                    else: sleep(0.2)
                except: sleep(0.2)
        except Exception as e:
            self.print_logs(f'Exception in select_language: {str(e)}')
            if self.DEBUG: print(f'Exception in select_language: {str(e)}')
            else: pass

    def select_brand(self, brand_name: str):
        flag = False
        self.wait_until_element_found(40, 'xpath', '//ul[@class="nav navbar-nav cc_navbar-nav"]/li//a[contains(text(), "Brands")]')
        try:
            brand_li = self.browser.find_element(By.XPATH, '//ul[@class="nav navbar-nav cc_navbar-nav"]/li//a[contains(text(), "Brands")]')
            ActionChains(self.browser).move_to_element(brand_li).perform()
            main_ul = self.browser.find_element(By.XPATH, '//ul[@data-value="Brands"]')
            for ul in main_ul.find_elements(By.TAG_NAME, 'ul'):
                if ul.get_attribute('id') in ['saf_pur', 'exc_pur', 'exc_visible']:
                    for li in ul.find_elements(By.TAG_NAME, 'li'):
                        if str(li.find_element(By.TAG_NAME, 'a').text).strip().lower() == str(brand_name).strip().lower():
                            li.click()
                            self.wait_until_browsing()
                            self.wait_for_page_loading()
                            flag = True
                            break
                if flag: break
        except Exception as e:
            self.print_logs(f'Exception in select_brand: {str(e)}')
            if self.DEBUG: print(f'Exception in select_brand: {str(e)}')
            else: pass
            flag = False
        finally: return flag
        
    def select_sunglasses_category(self, glasses_type: str):
        try:
            for a in self.browser.find_elements(By.XPATH, '//a[@class="cc_collapse_group"]'):
                if str('Product Type').strip().lower() in str(a.text).strip().lower():
                    a.click()
                    sleep(0.3)
                    xpath_glasses_type = ''
                    if glasses_type == 'Sunglasses': xpath_glasses_type = '//input[@data-value="SUN"]'
                    elif glasses_type == 'Eyeglasses': xpath_glasses_type = '//input[@data-value="EYE"]'
                    elif glasses_type == 'Ski & Snowboard Goggles': xpath_glasses_type = '//input[@data-value="SPO"]'
                    checkbox = self.browser.find_element(By.XPATH, xpath_glasses_type)
                    self.browser.execute_script("arguments[0].scrollIntoView();", checkbox)
                    ActionChains(self.browser).move_to_element(checkbox).click().perform()
                    sleep(0.2)
                    self.wait_for_page_loading()
                    break
        except Exception as e:
            self.print_logs(f'Exception in select_sunglasses_category: {str(e)}')
            if self.DEBUG: print(f'Exception in select_sunglasses_category: {str(e)}')
            else: pass

    def load_all_products(self):
        while True:
            try:
                self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.browser.find_element(By.XPATH, '//div[@class="row cc_list_footer"]/div/button').click()
                self.wait_for_page_loading()
                sleep(0.4)
            except: break

    def get_product_data(self, product_span, brand: Brand):
        product_url, product_name, product_number = '', '', ''
        try:
            product_url = product_span.find_element(By.XPATH, './/h5[@class="cc_product_link"]/a').get_attribute('href')
            text = str(product_span.find_element(By.XPATH, './/h5[@class="cc_product_link"]/a').text).strip()
            text = str(text).lower().replace(str(brand.name).strip().lower(), '').strip()
            text = str(text).lower().replace(str(brand.code).strip().lower(), '').strip()
            product_name = str(text).strip().title()
            product_number = str(product_span.find_element(By.XPATH, './/h5[@class="cc_product_link"]/a').get_attribute('data-id')).strip()
        except Exception as e:
            self.print_logs(f'Exception in get_product_data: {str(e)}')
            if self.DEBUG: print(f'Exception in get_product_data: {str(e)}')
            else: pass
        finally: return product_url, product_name, product_number

    def get_main_information(self):
        gender, frame_material, template_material, type_material, frame_shape = '', '', '', '', ''
        try:
            self.wait_until_element_found(30, 'xpath', '//a[@href="#product-main-info"]')
            
            element = self.browser.find_element(By.XPATH, '//a[@href="#product-main-info"]')
            ActionChains(self.browser).move_to_element(element).click().perform()
            sleep(0.5)

            for div in self.browser.find_elements(By.XPATH, '//div[@id="product-main-info"]/div[@class="row product_spec"]'):
                
                divs = div.find_elements(By.TAG_NAME, 'div')
                label = str(divs[0].text).strip()
                value = str(divs[1].text).strip()

                if str('Gender').strip().lower() in str(label).strip().lower(): gender = value
                elif str('Front Material').strip().lower() in str(label).strip().lower(): frame_material = value
                elif str('Temple Material').strip().lower() in str(label).strip().lower(): template_material = value
                elif str('Tip Material').strip().lower() in str(label).strip().lower(): type_material = value
                elif str('Shape').strip().lower() in str(label).strip().lower(): frame_shape = value
        except Exception as e:
            self.print_logs(f'Exception in get_product_details: {str(e)}')
            if self.DEBUG: print(f'Exception in get_product_details: {str(e)}')
            else: pass
        finally: return gender, frame_material, template_material, type_material, frame_shape

    def wait_until_browsing(self):
        while True:
            state = self.browser.execute_script('return document.readyState; ')
            if 'complete' == state: break
            else: sleep(0.2)

    def open_new_tab(self, url):
        # open category in new tab
        self.browser.execute_script('window.open("'+str(url)+'","_blank");')
        self.browser.switch_to.window(self.browser.window_handles[len(self.browser.window_handles) - 1])
        self.wait_until_browsing()
    
    def close_last_tab(self):
        self.browser.close()
        self.browser.switch_to.window(self.browser.window_handles[len(self.browser.window_handles) - 1])

    def get_total_products(self) -> int:
        total_products = 0
        try:
            total_products = len(self.browser.find_elements(By.XPATH, '//div[@class="productListContent cc_results_list cc_grid_container"]/span[@class="cc_product_container productFlexItem"]'))
        except Exception as e:
            self.print_logs(f'Exception in get_total_products: {str(e)}')
            if self.DEBUG: print(f'Exception in get_total_products: {str(e)}')
            else: pass
        finally: return total_products

    def clean_product_name(self, product: Product) -> None:
        try:
            if product.brand_id == 17 and 'CA ' in product.name: product.name = str(product.name).upper().replace('CA ', '').strip()
            if product.brand_id == 18 and 'CARDUC ' in product.name: product.name = str(product.name).upper().replace('CARDUC ', '').strip()
            if product.brand_id == 19 and 'CF ' in product.name: product.name = str(product.name).upper().replace('CF ', '').strip()
            if product.brand_id == 20 and 'DB ' in product.name: product.name = str(product.name).upper().replace('DB ', '').strip()
            if product.brand_id == 21 and 'PLD ' in product.name: product.name = str(product.name).upper().replace('PLD ', '').strip()
            if product.brand_id == 22 and 'MARC ' in product.name: product.name = str(product.name).upper().replace('MARC ', '').strip()
            elif product.brand_id == 22 and 'MJ ' in product.name: product.name = str(product.name).upper().replace('MJ ', '').strip()
        except Exception as e:
            if self.DEBUG: print(f'Exception in clean_product_name: {e}')
            self.print_logs(f'Exception in clean_product_name: {e}')

    def save_to_json(self, products: list[Product]):
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
            if self.DEBUG: print(f'Exception in save_to_json: {e}')
            self.print_logs(f'Exception in save_to_json: {e}')

    # print logs to the log file
    def print_logs(self, log: str) -> None:
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

    def printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r") -> None:
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

