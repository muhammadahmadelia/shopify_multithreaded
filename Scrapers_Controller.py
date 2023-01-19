import os
import sys
import json
import glob
import datetime
from datetime import datetime
import chromedriver_autoinstaller
import pandas as pd

from models.store import Store
from models.brand import Brand
from models.product import Product
from models.metafields import Metafields
from models.variant import Variant
from modules.files_reader import Files_Reader
from modules.query_processor import Query_Processor

from scrapers.digitalhub import Digitalhub_Scraper
from scrapers.safilo import Safilo_Scraper
from scrapers.keringeyewear import Keringeyewear_Scraper
from scrapers.rudyproject import Rudyproject_Scraper
from scrapers.luxottica import Luxottica_Scraper

from database.digitalhub import Digitalhub_Database
from database.safilo import Safilo_Database
from database.keringeyewear import Keringeyewear_Database
from database.rudyproject import Rudyproject_Database
from database.luxottica import Luxottica_Database

from shopify.digitalhub import Digitalhub_Shopify
from shopify.safilo import Safilo_Shopify
from shopify.keringeyewear import Keringeyewear_Shopify
from shopify.rudyproject import Rudyproject_Shopify
from shopify.luxottica import Luxottica_Shopify


class Controller:
    def __init__(self, DEBUG: bool, path: str) -> None:
        self.DEBUG = DEBUG
        self.store: Store = None
        self.path: str = path
        self.config_file: str = f'{self.path}/files/config.json'
        self.connection_file: str = f'{self.path}/files/conn.txt'
        self.results_foldername: str = ''
        self.logs_folder_path: str = ''
        self.result_filename: str = ''
        self.logs_filename: str = ''
        pass

    def controller(self) -> None:
        try:

            # reading connection file
            file_reader = Files_Reader(self.DEBUG)
            conn_string = file_reader.read_text_file(self.connection_file)

            # getting all stores from database
            query_processor = Query_Processor(self.DEBUG, conn_string)
            stores = query_processor.get_stores()

            if stores:
                self.manage_template_folder(stores, query_processor)

                self.store = self.get_store_to_update(stores)
                if self.store:
                    
                    self.results_foldername = f'{self.path}/scraped_data/{self.store.name}/'
                    self.logs_folder_path = f'{self.path}/Logs/{self.store.name}/'

                    if not os.path.exists('Logs'): os.makedirs('Logs')
                    if not os.path.exists(self.logs_folder_path): os.makedirs(self.logs_folder_path)
                    if not self.logs_filename: self.create_logs_filename()
                    
                    # getting all brands of store from database
                    brands = query_processor.get_brands_by_store_id(self.store.id)

                    # getting user selected brands to scrape and update
                    selected_brands = self.get_brands_to_update(brands)
                    
                    # getting all type of products for store
                    product_types = query_processor.get_product_types_by_store_id(self.store.id)

                    selected_brands_and_types = []
                    for selected_brand in selected_brands:
                        
                        # getting user selected product type for each brand 
                        selected_product_types = self.get_product_type_to_update(selected_brand, product_types)
                        
                        if selected_product_types:
                            self.store.brands = selected_brand
                            selected_brands_and_types.append({'brand': selected_brand, 'types': selected_product_types})

                        else: print(f'No product type selected for {selected_brand.name}')
                    
                    
                    if selected_brands_and_types:
                        if not os.path.exists('scraped_data'): os.makedirs('scraped_data')
                        if not os.path.exists(self.results_foldername): os.makedirs(self.results_foldername)
                        self.remove_extra_scraped_files()
                        self.create_result_filename()

                        print('\n')

                        if self.store.id == 1: Digitalhub_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store, selected_brands_and_types)
                        elif self.store.id == 2: Safilo_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store, selected_brands_and_types)
                        elif self.store.id == 3: Keringeyewear_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store, selected_brands_and_types)
                        elif self.store.id == 4: Rudyproject_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store, selected_brands_and_types)
                        elif self.store.id == 5: Luxottica_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store, selected_brands_and_types)

                        self.read_data_from_json_file(self.store.brands)

                        if self.store.id == 1: Digitalhub_Database(self.DEBUG, self.logs_filename).controller(self.store, query_processor)
                        if self.store.id == 2: Safilo_Database(self.DEBUG, self.logs_filename).controller(self.store, query_processor)
                        if self.store.id == 3: Keringeyewear_Database(self.DEBUG, self.logs_filename).controller(self.store, query_processor)
                        if self.store.id == 4: Rudyproject_Database(self.DEBUG, self.logs_filename).controller(self.store, query_processor)
                        if self.store.id == 5: Luxottica_Database(self.DEBUG, self.logs_filename).controller(self.store, query_processor)

                        self.empty_brand_products(self.store)
                        self.update_brand_inventory(self.store, query_processor)

                        self.get_all_products_for_brands(self.store, selected_brands_and_types, query_processor)
                        
                        shopify_obj = None
                        if self.store.id == 1: shopify_obj = Digitalhub_Shopify(self.DEBUG, self.config_file, query_processor, self.logs_filename)
                        if self.store.id == 2: shopify_obj = Safilo_Shopify(self.DEBUG, self.config_file, query_processor, self.logs_filename)
                        if self.store.id == 3: shopify_obj = Keringeyewear_Shopify(self.DEBUG, self.config_file, query_processor, self.logs_filename)
                        if self.store.id == 4: shopify_obj = Rudyproject_Shopify(self.DEBUG, self.config_file, query_processor, self.logs_filename)
                        if self.store.id == 5: shopify_obj = Luxottica_Shopify(self.DEBUG, self.config_file, query_processor, self.logs_filename)
                        
                        shopify_obj.controller(self.store.brands)

                        if shopify_obj.new_products: self.handle_new_products(shopify_obj, query_processor)
                        if shopify_obj.new_variants: self.handle_new_variants(shopify_obj, query_processor)
                        self.handle_found_and_not_found_products(self.store, shopify_obj, query_processor)

                
                else: print('No store selected to scrape')
            else: print('XAMPP is not started yet')
        except Exception as e:
            if self.DEBUG: print(f'Exception in Shopify_Controller controller: {e}')
            else: pass

        # create logs filename
    def create_logs_filename(self) -> None:
        try:
            scrape_time = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
            self.logs_filename = f'{self.logs_folder_path}Logs {scrape_time}.txt'
        except Exception as e:
            self.print_logs(f'Exception in create_logs_filename: {str(e)}')
            if self.DEBUG: print(f'Exception in create_logs_filename: {e}')
            else: pass

    # create result filename
    def create_result_filename(self) -> None:
        try:
            if not self.result_filename:
                scrape_time = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                self.result_filename = f'{self.results_foldername}Results {scrape_time}.json'
        except Exception as e:
            self.print_logs(f'Exception in create_result_filename: {str(e)}')
            if self.DEBUG: print(f'Exception in create_result_filename: {e}')
            else: pass

    # remove extra scraped files and keep latest 6 files 
    def remove_extra_scraped_files(self) -> None:
        try:
            files = glob.glob(f'{self.results_foldername}*.json')
            while len(files) > 5:
                oldest_file = min(files, key=os.path.getctime)
                os.remove(oldest_file)
                files = glob.glob(f'{self.results_foldername}*.json')
        except Exception as e:
            self.print_logs(f'Exception in remove_extra_scraped_files: {str(e)}')
            if self.DEBUG: print(f'Exception in remove_extra_scraped_files: {e}')
            else: pass

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

    def get_store_to_update(self, stores: list[Store]) -> Store:
        selected_store = None
        try:
            print('Select any store to update:')
            for store_index, store in enumerate(stores):
                print(store_index + 1, store.name)

            while True:
                store_choice = 0
                try:
                    store_choice = int(input('Choice: '))
                    if store_choice > 0 and store_choice <= len(stores):
                        selected_store = stores[int(store_choice) - 1]
                        break
                    else: print(f'Please enter number from 1 to {len(stores)}')
                except: print(f'Please enter number from 1 to {len(stores)}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store_to_update: {e}')
            else: pass
        finally: return selected_store

    def get_brands_to_update(self, brands: list[Brand]) -> list[Brand]:
        selected_brands = []
        try:
            print('\nSelect brands to scrape and update:')
            for brand_index, brand in enumerate(brands):
                print(brand_index + 1, brand.name)


            while True:
                brand_choices = ''
                try:
                    brand_choices = input('Choice: ')
                    if brand_choices:
                        for brand_choice in brand_choices.split(','):
                            selected_brands.append(brands[int(str(brand_choice).strip()) - 1])
                        break
                    else: print(f'Please enter number from 1 to {len(brands)}')
                except Exception as e:
                    print(f'Please enter number from 1 to {len(brands)}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands_to_update: {e}')
            else: pass
        finally: return selected_brands

    def get_product_type_to_update(self, brand: Brand, product_types: list[str]) -> list[str]:
        selected_product_types = []
        try:
            print(f'\nSelect product type to update for {brand.name}:')
            for product_type_index, product_type in enumerate(product_types):
                print(product_type_index + 1, str(product_type).title())


            while True:
                product_type_choices = ''
                try:
                    product_type_choices = input('Choice: ')
                    if product_type_choices:
                        for product_type_choice in product_type_choices.split(','):
                            selected_product_types.append(product_types[int(str(product_type_choice).strip()) - 1])
                        break
                    else: print(f'Product type cannot be empty')
                except Exception as e:
                    if self.DEBUG: print(e) 
                    print(f'Please enter number from 1 to {len(product_types)}')

        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_type_to_update: {e}')
            else: pass
        finally: return selected_product_types

    # read data from json file
    def read_data_from_json_file(self, brands: list[Brand]):
        try:
            files = glob.glob(f'{self.results_foldername}*.json')
            if files:
                latest_file = max(files, key=os.path.getctime)

                f = open(latest_file)
                json_data = json.loads(f.read())
                f.close()
                products = []
                for json_d in json_data:
                    product = Product()
                    product.brand_id = json_d['brand_id']
                    product.number = str(json_d['number']).strip().upper()
                    product.name = str(json_d['name']).strip().upper()
                    product.frame_code = str(json_d['frame_code']).strip().upper()
                    product.frame_color = str(json_d['frame_color']).strip().title()
                    product.lens_code = str(json_d['lens_code']).strip().upper()
                    product.lens_color = str(json_d['lens_color']).strip().title()
                    product.type = str(json_d['status']).strip().lower()
                    product.type = str(json_d['type']).strip().title()
                    product.url = str(json_d['url']).strip()
                    metafields = Metafields()
                    
                    for json_metafiels in json_d['metafields']:
                        if json_metafiels['key'] == 'for_who':metafields.for_who = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'product_size':metafields.product_size = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'activity':metafields.activity = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'lens_material':metafields.lens_material = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'graduabile':metafields.graduabile = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'interest':metafields.interest = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'lens_technology':metafields.lens_technology = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'frame_material':metafields.frame_material = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'frame_shape':metafields.frame_shape = str(json_metafiels['value']).strip().title()
                        elif json_metafiels['key'] == 'img_url':metafields.img_url = str(json_metafiels['value']).strip()
                        elif json_metafiels['key'] == 'img_360_urls':
                            value = str(json_metafiels['value']).strip()
                            if '[' in value: value = str(value).replace('[', '').strip()
                            if ']' in value: value = str(value).replace(']', '').strip()
                            if "'" in value: value = str(value).replace("'", '').strip()
                            for v in value.split(','):
                                metafields.img_360_urls = str(v).strip()
                    product.metafields = metafields
                    for json_variant in json_d['variants']:
                        variant = Variant()
                        variant.position = json_variant['position']
                        variant.title = str(json_variant['title']).strip()
                        variant.sku = str(json_variant['sku']).strip().upper()
                        variant.inventory_quantity = json_variant['inventory_quantity']
                        variant.found_status = json_variant['found_status']
                        try:
                            variant.listing_price = str(json_variant['listing_price']).strip()
                        except: variant.listing_price = str(json_variant['price']).strip()
                        variant.barcode_or_gtin = str(json_variant['barcode_or_gtin']).strip()
                        variant.size = str(json_variant['size']).strip()
                        variant.weight = str(json_variant['weight']).strip()
                        product.variants = variant 
                    products.append(product)

                for brand in brands:
                    for product in products:
                        if product.brand_id == brand.id:
                            brand.products = product
        except Exception as e:
            self.print_logs(f'Exception in read_data_from_json_file: {str(e)}')
            if self.DEBUG: print(f'Exception in read_data_from_json_file: {e}')
            else: pass

    # empty all products of brands for all stores
    def empty_brand_products(self, store: Store):
        try:
            for brand in store.brands: brand.empty_products()
        except Exception as e:
            self.print_logs(f'Exception in empty_brand_products: {str(e)}')
            if self.DEBUG: print(f'Exception in empty_brand_products: {e}')
            else: pass

    # update inventory quantity of not found products to 0
    def update_brand_inventory(self, store: Store, query_processor: Query_Processor) -> None:
        try:
            # update inventory quantity of not found products to 0
            for brand in store.brands:
                variants = query_processor.get_variants_by_brand_id(brand.id)
                for variant in variants:
                    if variant.found_status == 0 and variant.inventory_quantity != 0:
                        query_processor.update_variant_inventory_quantity(0, variant.id)
        except Exception as e:
            self.print_logs(f'Exception in update_brand_inventory: {str(e)}')
            if self.DEBUG: print(f'Exception in update_brand_inventory: {e}')
            else: pass

    # get all products for all brands of stores
    def get_all_products_for_brands(self, store: Store, brands_to_scrape: list[dict], query_processor: Query_Processor) -> None:
        try:
            for brand in store.brands:
                products: list[Product] = []
                for brand_to_scrape in brands_to_scrape:
                    if str(brand_to_scrape['brand'].name).strip().lower() == str(brand.name).strip().lower():
                        for index, category in enumerate(brand_to_scrape['types']):
                            condition = ''
                            condition += f'type = "{category}"'
                            
                            if condition: products += query_processor.get_products_by_brand_id(brand.id, condition)
                            else: products += query_processor.get_products_by_brand_id(brand.id)
                
                variants = query_processor.get_variants_by_brand_id(brand.id)
                metafields = query_processor.get_metafields_by_brand_id(brand.id)        
                
                # for product in products:
                #     product.metafields = query_processor.get_metafield_by_product_id(product.id)
                #     variants = query_processor.get_variants_by_product_id(product.id)
                #     for variant in variants: product.variants = variant
                #     brand.products = product
                for product in products:
                    for variant in variants:
                        if variant.product_id == product.id:
                            product.variants = variant

                    for metafield in metafields:
                        if metafield.product_id == product.id:
                            product.metafields = metafield
                    brand.products = product
        except Exception as e:
            self.print_logs(f'Exception in get_all_products_for_brands: {str(e)}')
            if self.DEBUG: print(f'Exception in get_all_products_for_brands: {e}')
            else: pass
    
        # handle new created products and create excel file for them
    def handle_new_products(self, shopify_obj: Luxottica_Shopify, query_processor: Query_Processor) -> None:
        try:
            data = []
            for new_product in shopify_obj.new_products:
                brand = query_processor.get_brand_by_id(new_product.brand_id)
                title = shopify_obj.create_product_title(brand, new_product)
                for new_variant in new_product.variants:
                    data.append([title, brand.name, new_product.type, new_variant.sku, new_variant.listing_price, new_variant.inventory_quantity])
            if data:
                columns = ['Title', 'Vendor', 'Product Type', 'Variant SKU', 'Price', 'Inventory Quantity']
                filename = 'New Products.xlsx'
                self.create_excel_file(data, columns, filename)
        except Exception as e:
            self.print_logs(f'Exception in handle_new_products: {str(e)}')
            if self.DEBUG: print(f'Exception in handle_new_products: {e}')
            else: pass

    # handle new created variants and create excel file for them
    def handle_new_variants(self, shopify_obj: Luxottica_Shopify, query_processor: Query_Processor) -> None:
        try:
            data = []
            for new_variant in shopify_obj.new_variants:
                product = query_processor.get_product_by_id(new_variant.product_id)
                brand = query_processor.get_brand_by_id(product.brand_id)
                title = shopify_obj.create_product_title(brand, product)
                data.append([title, brand.name, product.type, new_variant.sku, new_variant.listing_price, new_variant.inventory_quantity])
            if data:
                columns = ['Title', 'Vendor', 'Product Type', 'Variant SKU', 'Price', 'Inventory Quantity']
                filename = 'New Variants.xlsx'
                self.create_excel_file(data, columns, filename)   
        except Exception as e:
            self.print_logs(f'Exception in handle_new_variants: {str(e)}')
            if self.DEBUG: print(f'Exception in handle_new_variants: {e}')
            else: pass

    # handle found and not found products and create excel file for them
    def handle_found_and_not_found_products(self, store: Store, shopify_obj: Luxottica_Shopify, query_processor: Query_Processor) -> None:
        try:
            for brand in store.brands:
                not_found_products = []
                found_products = []
                created_time = datetime.now().strftime('%d-%m-%Y')
                for product in query_processor.get_products_by_brand_id_and_created_time(brand.id, created_time):
                    title = shopify_obj.create_product_title(brand, product)
                    for variant in query_processor.get_variants_by_product_id_and_created_time(product.id, created_time):
                        if variant.found_status == 0: not_found_products.append([title, brand.name, product.type, variant.sku, variant.listing_price, variant.inventory_quantity])
                        else: found_products.append([title, brand.name, product.type, variant.sku, variant.listing_price, variant.inventory_quantity])

                if found_products:
                    columns = ['Title', 'Vendor', 'Product Type', 'Variant SKU', 'Price', 'Inventory Quantity']
                    filename = 'Found Products.xlsx'
                    self.create_excel_file(found_products, columns, filename)
                if not_found_products:
                    columns = ['Title', 'Vendor', 'Product Type', 'Variant SKU', 'Price', 'Inventory Quantity']
                    filename = 'Not found Products.xlsx'
                    self.create_excel_file(not_found_products, columns, filename)
        except Exception as e:
            self.print_logs(f'Exception in handle_found_and_not_found_products: {str(e)}')
            if self.DEBUG: print(f'Exception in handle_found_and_not_found_products: {e}')
            else: pass

    # create excel file with provided data and columns
    def create_excel_file(self, data: list[str], columns: list[str], filename: str) -> None:
        try:
            df = pd.DataFrame(data, columns=columns)
            df.to_excel(f'{str(filename)}', sheet_name='Products', index=False, header=True)
        except Exception as e:
            self.print_logs(f'Exception in create_excel: {str(e)}')
            if self.DEBUG: print(f'Exception in create_excel: {e}')
            else: pass

    # manage templates folder - create those folder and files which are needed and not created yet 
    def manage_template_folder(self, stores: list[Store], query_processor: Query_Processor) -> None:
        try:
            files = ['title.txt', 'product_description.txt', 'meta_title.txt', 'meta_description.txt', 'image_description.txt']
            
            if not os.path.exists('templates'): os.makedirs('templates')

            for store in stores:
            
                STORE_FOLDER_PATH = F'templates/{str(store.name).strip().title()}'
                if not os.path.exists(STORE_FOLDER_PATH): os.makedirs(STORE_FOLDER_PATH)

                # getting all brands of store from database
                brands = query_processor.get_brands_by_store_id(store.id)

                for brand in brands:
                    
                    BRAND_FOLDER_PATH = f'{STORE_FOLDER_PATH}/{str(brand.name).strip().title()}'
                    if not os.path.exists(BRAND_FOLDER_PATH): os.makedirs(BRAND_FOLDER_PATH)
                    
                    # getting all type of products for store
                    product_types = query_processor.get_product_types_by_store_id(store.id)

                    for product_type in product_types:

                        PRODUCT_TYPE_FOLDER_PATH = f'{BRAND_FOLDER_PATH}/{str(product_type).strip().title()}'
                        if not os.path.exists(PRODUCT_TYPE_FOLDER_PATH): os.makedirs(PRODUCT_TYPE_FOLDER_PATH)

                        for file in files:
                            FILE_PATH = f'{PRODUCT_TYPE_FOLDER_PATH}/{file}'
                            if not os.path.exists(FILE_PATH):
                                f = open(FILE_PATH, "x")
                                f.close()


        except Exception as e:
            self.print_logs(f'Exception in manage_template_folder: {e}')
            if self.DEBUG: print(f'Exception in manage_template_folder: {e}')

DEBUG = True
try:
    pathofpyfolder = os.path.realpath(sys.argv[0])
    # get path of Exe folder
    path = pathofpyfolder.replace(pathofpyfolder.split('\\')[-1], '')
    # download chromedriver.exe with same version and get its path
    if os.path.exists('chromedriver.exe'): os.remove('chromedriver.exe')
    if os.path.exists('Luxottica Results.xlsx'): os.remove('Luxottica Results.xlsx')

    chromedriver_autoinstaller.install(path)
    if '.exe' in pathofpyfolder.split('\\')[-1]: DEBUG = False
    
    

    Controller(DEBUG, path).controller()

except Exception as e:
    if DEBUG: print('Exception: '+str(e))
    else: pass