from models.store import Store
from models.product import Product
from models.variant import Variant
from modules.query_processor import Query_Processor

class Keringeyewear_Database:
    def __init__(self, DEBUG: bool, logs_filename: str) -> None:
        self.DEBUG = DEBUG
        self.logs_filename = logs_filename
        pass

    def controller(self, store: Store, query_processor: Query_Processor) -> None:
        try:
            print('Updating Database for')
            for brand in store.brands:

                new_products_by_brand = 0
                if len(brand.products) > 0:
                    print(f'Brand: {brand.name} | No. of Products {len(brand.products)}')
                    self.printProgressBar(0, len(brand.products), prefix = 'Progress:', suffix = 'Complete', length = 50)

                    # update all variants of variant found status to 0
                    variants_by_brand_from_database =  query_processor.get_variants_by_brand_id(brand.id)
                    for variant_by_brand_from_database in variants_by_brand_from_database:
                            query_processor.update_variant_found_status(0, variant_by_brand_from_database.id)

                    database_products_by_brand =  query_processor.get_products_by_brand_id(brand.id)
                    
                    for scraped_product_by_brand_index, scraped_product_by_brand in enumerate(brand.products):
                        new_variants_by_product = 0 
                        matched_product_from_database = self.match_scraped_product_with_database_products(scraped_product_by_brand, database_products_by_brand)

                        if str(matched_product_from_database.id).strip() != '0':
                            # check product name #
                            if str(scraped_product_by_brand.name).strip():
                                # when scraped_product name is not equal to database_product name
                                if str(scraped_product_by_brand.name).strip().upper() != str(matched_product_from_database.name).strip().upper():
                                    # update scraped_product name as new name
                                    query_processor.update_product_name(str(scraped_product_by_brand.name).strip().upper(), matched_product_from_database.id)
                                    if self.DEBUG: print('name', matched_product_from_database.name, scraped_product_by_brand.name)

                            # check product frame_color #
                            if str(scraped_product_by_brand.frame_color).strip():
                                # when scraped_product frame_color is not equal to database_product frame_color
                                if str(scraped_product_by_brand.frame_color).strip().title() != str(matched_product_from_database.frame_color).strip().title():
                                    # update scraped_product frame_color as new frame_color
                                    query_processor.update_product_frame_color(str(scraped_product_by_brand.frame_color).strip().title(), matched_product_from_database.id)
                                    if self.DEBUG: print('frame_color', matched_product_from_database.frame_color, scraped_product_by_brand.frame_color)

                            # check product lens_code #
                            if str(scraped_product_by_brand.lens_code).strip():
                                # when scraped_product lens_code is not equal to database_product lens_color
                                if str(scraped_product_by_brand.lens_code).strip().title() != str(matched_product_from_database.lens_code).strip().title():
                                    # update scraped_product lens_code as new lens_code
                                    query_processor.update_product_lens_code(str(scraped_product_by_brand.lens_code).strip().title(), matched_product_from_database.id)
                                    if self.DEBUG: print('lens_code', matched_product_from_database.lens_code, scraped_product_by_brand.lens_code)

                            # check product lens_color #
                            if str(scraped_product_by_brand.lens_color).strip():
                                # when scraped_product lens_color is not equal to database_product lens_color
                                if str(scraped_product_by_brand.lens_color).strip().title() != str(matched_product_from_database.lens_color).strip().title():
                                    # update scraped_product lens_color as new lens_color
                                    query_processor.update_product_lens_color(str(scraped_product_by_brand.lens_color).strip().title(), matched_product_from_database.id)
                                    if self.DEBUG: print('lens_color', matched_product_from_database.lens_color, scraped_product_by_brand.lens_color)

                            # check product url #
                            if str(scraped_product_by_brand.url).strip():
                                # when scraped_product url is not equal to database_product url
                                if str(scraped_product_by_brand.url).strip() != str(matched_product_from_database.url).strip():
                                    # update scraped_product url as new url
                                    query_processor.update_product_url(str(scraped_product_by_brand.url).strip(), matched_product_from_database.id)
                                    if self.DEBUG: print('url', matched_product_from_database.url, scraped_product_by_brand.url)

                            # get variants from database against product id
                            database_variants_by_product = query_processor.get_variants_by_product_id(matched_product_from_database.id)

                            

                            for scraped_variant_by_product in scraped_product_by_brand.variants:
                                matched_variant_from_database = self.match_scraped_variant_with_database_variants(scraped_variant_by_product, database_variants_by_product)

                                if str(matched_variant_from_database.id).strip() != '0':

                                    # change variant found status #
                                    query_processor.update_variant_found_status(1, matched_variant_from_database.id)

                                    # check variant inventory qunatity #
                                    if int(scraped_variant_by_product.inventory_quantity) != int(matched_variant_from_database.inventory_quantity):
                                        query_processor.update_variant_inventory_quantity(int(scraped_variant_by_product.inventory_quantity), matched_variant_from_database.id)
                                        if self.DEBUG: print('inventory_quantity', matched_variant_from_database.inventory_quantity, scraped_variant_by_product.inventory_quantity)

                                    # check variant price #
                                    if str(scraped_variant_by_product.listing_price).strip():
                                        if str(scraped_variant_by_product.listing_price).strip() != str(matched_variant_from_database.listing_price).strip():
                                            query_processor.update_variant_price(str(scraped_variant_by_product.listing_price).strip(), matched_variant_from_database.id)
                                            if self.DEBUG: print('price', matched_variant_from_database.listing_price, scraped_variant_by_product.listing_price)

                                    # check variant barcode #
                                    if str(scraped_variant_by_product.barcode_or_gtin).strip():
                                        scraped_product_by_brand.metafields.gtin1 += f'{str(scraped_variant_by_product.barcode_or_gtin).strip()}, '
                                        if str(scraped_variant_by_product.barcode_or_gtin).strip() != str(matched_variant_from_database.barcode_or_gtin).strip():
                                            query_processor.update_variant_barcode(str(scraped_variant_by_product.barcode_or_gtin).strip(), matched_variant_from_database.id)
                                            if self.DEBUG: print('barcode_or_gtin', matched_variant_from_database.barcode_or_gtin, scraped_variant_by_product.barcode_or_gtin)

                                    # check variant size #
                                    if str(scraped_variant_by_product.size).strip() and str(scraped_variant_by_product.size).strip() != '-0-0':
                                        if str(scraped_variant_by_product.size).strip() != str(matched_variant_from_database.size).strip():
                                            query_processor.update_variant_size(str(scraped_variant_by_product.size).strip(), matched_variant_from_database.id)
                                            if self.DEBUG: print('size', matched_variant_from_database.size, scraped_variant_by_product.size)
                                else:
                                    position = len(query_processor.get_variants_by_product_id(matched_product_from_database.id)) + 1
                                    query_processor.insert_variant(matched_product_from_database.id, position, scraped_variant_by_product)
                                    new_variants_by_product += 1
                                    
                            # get metafield from database against product id
                            database_metafield_by_product = query_processor.get_metafield_by_product_id(matched_product_from_database.id)

                            if str(database_metafield_by_product.id).strip() != '0':

                                scraped_product_by_brand.metafields.gtin1 = ''
                                scraped_product_by_brand.metafields.product_size = ''

                                if len(scraped_product_by_brand.variants) == 1:
                                    scraped_product_by_brand.metafields.product_size = str(scraped_product_by_brand.variants[0].size).strip()
                                    scraped_product_by_brand.metafields.gtin1 = str(scraped_product_by_brand.variants[0].barcode_or_gtin).strip()
                                else:
                                    for scraped_variant_by_product in scraped_product_by_brand.variants:
                                        if str(scraped_variant_by_product.barcode_or_gtin).strip():
                                            scraped_product_by_brand.metafields.gtin1 += f'{str(scraped_variant_by_product.barcode_or_gtin).strip()}, '
                                    if str(scraped_product_by_brand.metafields.gtin1).strip() and str(scraped_product_by_brand.metafields.gtin1).strip()[-1] == ',': 
                                        scraped_product_by_brand.metafields.gtin1 = str(scraped_product_by_brand.metafields.gtin1).strip()[:-1]

                                # check for_who metafield #
                                if str(scraped_product_by_brand.metafields.for_who).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.for_who):
                                    if str(scraped_product_by_brand.metafields.for_who).strip().lower() != str(database_metafield_by_product.for_who).strip().lower():
                                        query_processor.update_for_who_metafield(str(scraped_product_by_brand.metafields.for_who).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('for_who', database_metafield_by_product.for_who, scraped_product_by_brand.metafields.for_who)

                                # check product_size  metafield #
                                if str(scraped_product_by_brand.metafields.product_size).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.product_size):
                                    if str(scraped_product_by_brand.metafields.product_size).strip() != str(database_metafield_by_product.product_size).strip():
                                        query_processor.update_product_size_metafield(str(scraped_product_by_brand.metafields.product_size).strip(), database_metafield_by_product.id)
                                        if self.DEBUG: print('product_size', database_metafield_by_product.product_size, scraped_product_by_brand.metafields.product_size)

                                # check activity metafield #
                                if str(scraped_product_by_brand.metafields.activity).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.activity):
                                    if str(scraped_product_by_brand.metafields.activity).strip().lower() != str(database_metafield_by_product.activity).strip().lower():
                                        query_processor.update_activity_metafield(str(scraped_product_by_brand.metafields.activity).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('activity', database_metafield_by_product.activity, scraped_product_by_brand.metafields.activity)

                                # check lens_material metafield #
                                if str(scraped_product_by_brand.metafields.lens_material).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.lens_material):
                                    if str(scraped_product_by_brand.metafields.lens_material).strip().lower() != str(database_metafield_by_product.lens_material).strip().lower():
                                        query_processor.update_lens_material_metafield(str(scraped_product_by_brand.metafields.lens_material).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('lens_material', database_metafield_by_product.lens_material, scraped_product_by_brand.metafields.lens_material)

                                # check graduabile metafield #
                                if str(scraped_product_by_brand.metafields.graduabile).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.graduabile):
                                    if str(scraped_product_by_brand.metafields.graduabile).strip().lower() != str(database_metafield_by_product.graduabile).strip().lower():
                                        query_processor.update_graduabile_metafield(str(scraped_product_by_brand.metafields.graduabile).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('graduabile', database_metafield_by_product.graduabile, scraped_product_by_brand.metafields.graduabile)

                                # check interest metafield #
                                if str(scraped_product_by_brand.metafields.interest).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.interest):
                                    if str(scraped_product_by_brand.metafields.interest).strip().lower() != str(database_metafield_by_product.interest).strip().lower():
                                        query_processor.update_interest_metafield(str(scraped_product_by_brand.metafields.interest).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('interest', database_metafield_by_product.interest, scraped_product_by_brand.metafields.interest)

                                # check lens_technology metafield #
                                if str(scraped_product_by_brand.metafields.lens_technology).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.lens_technology):
                                    if str(scraped_product_by_brand.metafields.lens_technology).strip().lower() != str(database_metafield_by_product.lens_technology).strip().lower():
                                        query_processor.update_lens_technology_metafield(str(scraped_product_by_brand.metafields.lens_technology).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('lens_technology', database_metafield_by_product.lens_technology, scraped_product_by_brand.metafields.lens_technology)

                                # check frame_material metafield #
                                if str(scraped_product_by_brand.metafields.frame_material).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.frame_material):
                                    if str(scraped_product_by_brand.metafields.frame_material).strip().lower() != str(database_metafield_by_product.frame_material).strip().lower():
                                        query_processor.update_frame_material_metafield(str(scraped_product_by_brand.metafields.frame_material).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('frame_material', database_metafield_by_product.frame_material, scraped_product_by_brand.metafields.frame_material)

                                # check frame_shape metafield #
                                if str(scraped_product_by_brand.metafields.frame_shape).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.frame_shape):
                                    if str(scraped_product_by_brand.metafields.frame_shape).strip().lower() != str(database_metafield_by_product.frame_shape).strip().lower():
                                        query_processor.update_frame_shape_metafield(str(scraped_product_by_brand.metafields.frame_shape).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('frame_shape', database_metafield_by_product.frame_shape, scraped_product_by_brand.metafields.frame_shape)

                                # check gtin1 metafield #
                                if str(scraped_product_by_brand.metafields.gtin1).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.gtin1):
                                    if str(scraped_product_by_brand.metafields.gtin1).strip().lower() != str(database_metafield_by_product.gtin1).strip().lower():
                                        if str(scraped_product_by_brand.metafields.gtin1).strip()[-1] == ',': scraped_product_by_brand.metafields.gtin1 = str(scraped_product_by_brand.metafields.gtin1).strip()[0:-1]
                                        query_processor.update_gtin1_metafield(str(scraped_product_by_brand.metafields.gtin1).strip().title(), database_metafield_by_product.id)
                                        if self.DEBUG: print('gtin1', database_metafield_by_product.gtin1, scraped_product_by_brand.metafields.gtin1)

                                # check img_url metafield #
                                if str(scraped_product_by_brand.metafields.img_url).strip() and 'property object at' not in str(scraped_product_by_brand.metafields.img_url):
                                    if str(scraped_product_by_brand.metafields.img_url).strip() != str(database_metafield_by_product.img_url).strip():
                                        query_processor.update_img_url_metafield(str(scraped_product_by_brand.metafields.img_url).strip(), database_metafield_by_product.id)
                                        if self.DEBUG: print('img_url', database_metafield_by_product.img_url, scraped_product_by_brand.metafields.img_url)

                                # check img_360_urls metafield #
                                if len(scraped_product_by_brand.metafields.img_360_urls) > 0 and 'property object at' not in str(scraped_product_by_brand.metafields.img_360_urls):
                                    scraped_product_img_360_urls = self.get_img_360_string(scraped_product_by_brand.metafields.img_360_urls)
                                    database_product_img_360_urls = self.get_img_360_string(database_metafield_by_product.img_360_urls)
                                    if str(scraped_product_img_360_urls).strip() != str(database_product_img_360_urls).strip():
                                        query_processor.update_img_360_urls_metafield(str(scraped_product_img_360_urls).strip(), database_metafield_by_product.id)
                                        if self.DEBUG: print('img_360_urls', database_product_img_360_urls, scraped_product_img_360_urls)
                            else:
                                # when metafields not found for product id then add new metafields
                                query_processor.insert_metafield(matched_product_from_database.id, scraped_product_by_brand.metafields)
                        else:
                            inserted_product_id = query_processor.insert_product(brand.id, scraped_product_by_brand)
                            if inserted_product_id != 0:
                                query_processor.insert_metafield(inserted_product_id, scraped_product_by_brand.metafields)
                                for index, scraped_variant in enumerate(scraped_product_by_brand.variants):
                                    position = index + 1
                                    query_processor.insert_variant(inserted_product_id, position, scraped_variant)
                                new_products_by_brand += 1

                        self.printProgressBar(scraped_product_by_brand_index + 1, len(brand.products), prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            if self.DEBUG: print(f'Exception in Database_Upater controller: {e}')
            self.print_logs(f'Exception in Database_Upater controller: {e}')

    # match scraped product with database products and return result
    def match_scraped_product_with_database_products(self, scraped_product_by_brand: Product, database_products_by_brand: list[Product]) -> Product:
        matched_product_from_database = Product()
        try:
            for database_product_by_brand in database_products_by_brand:
                database_product_number = str(database_product_by_brand.number).strip().upper().replace('/', '-')
                scraped_product_number = str(scraped_product_by_brand.number).strip().upper().replace('/', '-')
                database_product_frame_code = str(database_product_by_brand.frame_code).strip().upper().replace('/', '-')
                scraped_product_frame_code = str(scraped_product_by_brand.frame_code).strip().upper().replace('/', '-')
                if database_product_number == scraped_product_number and database_product_frame_code == scraped_product_frame_code:
                    matched_product_from_database = database_product_by_brand
                    break
        except Exception as e:
            self.print_logs(f'Exception in match_scraped_product_with_database_products: {str(e)}')
            if self.DEBUG: print(f'Exception in match_scraped_product_with_database_products: {e}')
            else: pass
        finally: return matched_product_from_database

    # match scraped variant with database variants and return matched result
    def match_scraped_variant_with_database_variants(self, scraped_variant_by_product: Variant, database_variants_by_product: list[Variant]) -> Variant:
        matched_variant_from_database = Variant()
        try:
            if len(database_variants_by_product) == 1:
                database_variant_sku = str(database_variants_by_product[0].sku).strip().upper().replace('/', '-')
                scraped_variant_sku = str(scraped_variant_by_product.sku).strip().upper().replace('/', '-')
                if len(database_variant_sku.split(' ')) == len(scraped_variant_sku.split(' ')):
                    if database_variant_sku == scraped_variant_sku:
                            matched_variant_from_database = database_variants_by_product[0]
                else:
                    if len(database_variant_sku.split(' ')) > len(scraped_variant_sku.split(' ')):
                        database_variant_sku = database_variant_sku.rsplit(' ', 1)[0].strip()
                    else:
                        scraped_variant_sku = scraped_variant_sku.rsplit(' ', 1)[0].strip()
                    
                    if database_variant_sku == scraped_variant_sku:
                            matched_variant_from_database = database_variants_by_product[0]
                    # else:
                    #     print(database_variant_sku, scraped_variant_sku)
            else:
                for database_variant_by_product in database_variants_by_product:
                    database_variant_sku = str(database_variant_by_product.sku).strip().upper().replace('/', '-')
                    scraped_variant_sku = str(scraped_variant_by_product.sku).strip().upper().replace('/', '-')

                    # if len(str(scraped_variant_sku).split(' ')) == 2:
                    #     database_variant_sku = str(database_variant_sku).replace(str(database_variant_sku).split(' ')[-1].strip(), '').strip()

                    if len(database_variant_sku.split(' ')) == len(scraped_variant_sku.split(' ')):
                        if database_variant_sku == scraped_variant_sku:
                            matched_variant_from_database = database_variant_by_product
                            break
                    else:
                        if str(database_variant_by_product.barcode_or_gtin).strip() == str(scraped_variant_by_product.sku).strip():
                            matched_variant_from_database = database_variant_by_product
                            break
        except Exception as e:
            self.print_logs(f'Exception in match_scraped_variant_with_database_variants: {str(e)}')
            if self.DEBUG: print(f'Exception in match_scraped_variant_with_database_variants: {e}')
            else: pass
        finally: return matched_variant_from_database

    # get 360 image urls in string
    def get_img_360_string(self, img_360_urls: list[str]) -> str:
        img_360_url_string = ''
        try:
            for img_360_url in img_360_urls:
                img_360_url_string += f'{str(img_360_url).strip()}, '

            if str(img_360_url_string).strip() and str(img_360_url_string).strip()[-1] == ',': img_360_url_string = str(img_360_url_string).strip()[0:-1]
            img_360_url_string = str(img_360_url_string).strip().replace('[', '').replace(']', '').replace("'", '')

        except Exception as e:
            self.print_logs(f'Exception in get_img_360_string: {str(e)}')
            if self.DEBUG: print(f'Exception in get_img_360_string: {e}')
            else: pass
        finally: return img_360_url_string

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

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