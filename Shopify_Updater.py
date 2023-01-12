import os
import sys
from datetime import datetime
from models.store import Store
from models.brand import Brand
from models.product import Product
from models.variant import Variant
from models.metafields import Metafields
from modules.files_reader import Files_Reader
from modules.query_processor import Query_Processor

from shopify.shopify_processor import Shopify_Processor


class Shopify_Updater:
    def __init__(self, DEBUG: bool, config_file: str, store: Store, logs_filename: str) -> None:
        self.DEBUG: bool = DEBUG
        self.config_file: str = config_file
        self.template_file_path: str = f'templates/{store.name}/'
        self.logs_filename: str = logs_filename
        pass

    def controller(self, fileds: list[str], brand: Brand) -> None:
        try:
            shopify_processor = Shopify_Processor(self.DEBUG, self.config_file, self.logs_filename)
            shopify_processor.get_store_url()
        
            print(f'Updating Shopify for {brand.name} | No. of Products: {len(brand.products)}')

            shopify_products = shopify_processor.get_products_by_vendor(brand.name)

            for product in brand.products:

                if product.shopify_id:
                    
                    # get matched product between shopify products nad dataabase product
                    shopify_product = self.get_matched_product(product.shopify_id, shopify_products)

                    if shopify_product and 'Outlet' not in shopify_product['tags']:

                        new_product_title = ''

                        if 'Product Title' in fileds:
                            title_template_path = self.get_template_path('Product Title', brand, product)
                            title_template = self.get_template(title_template_path)
                            title = self.get_original_text(title_template, brand, product)
                            new_product_title = title
                            if self.DEBUG: print(new_product_title)
                            if title: self.check_product_title(title, product, shopify_product, shopify_processor)
                        
                        if 'Product Description' in fileds:
                            product_description_template_path = self.get_template_path('Product Description', brand, product)
                            product_description_template = self.get_template(product_description_template_path)
                            product_description = self.get_original_text(product_description_template, brand, product)
                            if self.DEBUG: print(product_description)
                            if product_description: self.check_product_description(product_description, product, shopify_product, shopify_processor)

                        if 'Meta Title' in fileds or 'Meta Description' in fileds:
                            shopify_metafields = shopify_processor.get_product_metafields_from_shopify(product.shopify_id)

                            if 'Meta Title' in fileds:
                                meta_title_template_path = self.get_template_path('Meta Title', brand, product)
                                meta_title_template = self.get_template(meta_title_template_path)
                                meta_title = self.get_original_text(meta_title_template, brand, product)

                                if meta_title:
                                    meta_title = str(meta_title).replace('  ', ' ').strip()
                                    if len(meta_title) > 60: meta_title = str(meta_title).replace('| LookerOnline', '| LO')
                                    if self.DEBUG: print(meta_title)
                                    self.check_product_meta_title(meta_title, new_product_title, product, shopify_metafields, shopify_processor)
                                    
                            
                            if 'Meta Description' in fileds:
                                meta_description_template_path = self.get_template_path('Meta Description', brand, product)
                                meta_description_template = self.get_template(meta_description_template_path)
                                meta_description = self.get_original_text(meta_description_template, brand, product)

                                if meta_description:
                                    meta_description = str(meta_description).replace('  ', ' ').replace('âœ“', '✓').strip()
                                    if self.DEBUG: print(meta_description)
                                    self.check_product_meta_description(meta_description, new_product_title, product, shopify_metafields, shopify_processor)
                        
                        if 'Image Description' in fileds:
                            image_description_template_path = self.get_template_path('Image Description', brand, product)
                            image_description_template = self.get_template(image_description_template_path)
                            image_description = self.get_original_text(image_description_template, brand, product)
                            if image_description:
                                if self.DEBUG: print(image_description)
                                self.check_product_image_description(image_description, shopify_product, shopify_processor, new_product_title)
                

        except Exception as e:
            self.print_logs(f'Exception in Shopify_Updater controller: {e}')
            if self.DEBUG: print(f'Exception in Shopify_Updater controller: {e}')
            else: pass

    # get product template path 
    def get_template_path(self, field: str, brand: Brand, product: Product) -> str:
        template_path = ''
        try:
            if field == 'Product Title': template_path = f'{self.template_file_path}{brand.name}/{product.type}/title.txt'
            elif field == 'Product Description': template_path = f'{self.template_file_path}{brand.name}/{product.type}/product_description.txt'
            elif field == 'Meta Title': template_path = f'{self.template_file_path}{brand.name}/{product.type}/meta_title.txt'
            elif field == 'Meta Description': template_path = f'{self.template_file_path}{brand.name}/{product.type}/meta_description.txt'
            elif field == 'Image Description': template_path = f'{self.template_file_path}{brand.name}/{product.type}/image_description.txt'
            
        except Exception as e:
            self.print_logs(f'Exception in get_template_path: {e}')
            if self.DEBUG: print(f'Exception in get_template_path: {e}')
        finally: return template_path

    # get product description template
    def get_template(self, path: str) -> str:
        template = ''
        try:
            if os.path.exists(path):
                file_reader = Files_Reader(self.DEBUG)
                template = file_reader.read_text_file(path)
        except Exception as e:
            self.print_logs(f'Exception in get_template: {e}')
            if self.DEBUG: print(f'Exception in get_template: {e}')
        finally: return template

    def get_original_text(self, template: str, brand: Brand, product: Product) -> str:
        try:
            if '{Brand.Name}' in template: template = str(template).replace('{Brand.Name}', str(brand.name).strip().title()).strip()
            elif '{BRAND.NAME}' in template: template = str(template).replace('{BRAND.NAME}', str(brand.name).strip().upper()).strip()
            elif '{brand.name}' in template: template = str(template).replace('{brand.name}', str(brand.name).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Number}' in template: template = str(template).replace('{Product.Number}', str(product.number).strip().title()).strip()
            elif '{PRODUCT.NUMBER}' in template: template = str(template).replace('{PRODUCT.NUMBER}', str(product.number).strip().upper()).strip()
            elif '{product.number}' in template: template = str(template).replace('{product.number}', str(product.number).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()
            
            if str(product.name).strip(): 
                if '{Product.Name}' in template: template = str(template).replace('{Product.Name}', str(product.name).strip().title())
                elif '{PRODUCT.NAME}' in template: template = str(template).replace('{PRODUCT.NAME}', str(product.name).strip().upper())
                elif '{product.name}' in template: template = str(template).replace('{product.name}', str(product.name).strip().lower())
            else: 
                if '{Product.Name}' in template: template = str(template).replace('{Product.Name}', '')
                elif '{PRODUCT.NAME}' in template: template = str(template).replace('{PRODUCT.NAME}', '')
                elif '{product.name}' in template: template = str(template).replace('{product.name}', '')
            template = str(template).replace('  ', ' ').strip()
            
            if '{Product.Frame_Code}' in template: template = str(template).replace('{Product.Frame_Code}', str(product.frame_code).strip().upper()).strip()
            elif '{PRODUCT.FRAME_CODE}' in template: template = str(template).replace('{PRODUCT.FRAME_CODE}', str(product.frame_code).strip().upper()).strip()
            elif '{product.frame_code}' in template: template = str(template).replace('{product.frame_code}', str(product.frame_code).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Frame_Color}' in template: template = str(template).replace('{Product.Frame_Color}', str(product.frame_color).strip().title()).strip()
            elif '{PRODUCT.FRAME_COLOR}' in template: template = str(template).replace('{PRODUCT.FRAME_COLOR}', str(product.frame_color).strip().upper()).strip()
            elif '{product.frame_color}' in template: template = str(template).replace('{product.frame_color}', str(product.frame_color).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Lens_Code}' in template: template = str(template).replace('{Product.Lens_Code}', str(product.lens_code).strip().title()).strip()
            elif '{PRODUCT.LENS_CODE}' in template: template = str(template).replace('{PRODUCT.LENS_CODE}', str(product.lens_code).strip().upper()).strip()
            elif '{product.lens_code}' in template: template = str(template).replace('{product.lens_code}', str(product.lens_code).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Lens_Color}' in template: template = str(template).replace('{Product.Lens_Color}', str(product.lens_color).strip().title()).strip()
            elif '{PRODUCT.LENS_COLOR}' in template: template = str(template).replace('{PRODUCT.LENS_COLOR}', str(product.lens_color).strip().upper()).strip()
            elif '{product.lens_color}' in template: template = str(template).replace('{product.lens_color}', str(product.lens_color).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Type}' in template: template = str(template).replace('{Product.Type}', str(product.type).strip().title()).strip()
            elif '{PRODUCT.TYPE}' in template: template = str(template).replace('{PRODUCT.TYPE}', str(product.type).strip().upper()).strip()
            elif '{product.type}' in template: template = str(template).replace('{product.type}', str(product.type).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            # Metafields
            if '{Product.Metafields.For_Who}' in template: template = str(template).replace('{Product.Metafields.For_Who}', str(product.metafields.for_who).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.FOR_WHO}' in template: template = str(template).replace('{PRODUCT.METAFIELDS.FOR_WHO}', str(product.metafields.for_who).strip().upper()).strip()
            elif '{product.metafields.for_who}' in template: template = str(template).replace('{product.metafields.for_who}', str(product.metafields.for_who).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Metafields.Lens_Material}' in template: template = str(template).replace('{Product.Metafields.Lens_Material}', str(product.metafields.lens_material).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.LENS_MATERIAL}' in template: template = str(template).replace('{PRODUCT.METAFIELDS.LENS_MATERIAL}', str(product.metafields.lens_material).strip().upper()).strip()
            elif '{product.metafields.lens_material}' in template: template = str(template).replace('{product.metafields.lens_material}', str(product.metafields.lens_material).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Metafields.Lens_Technology}' in template: template = str(template).replace('{Product.Metafields.Lens_Technology}', str(product.metafields.lens_technology).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.LENS_TECHNOLOGY}' in template: template = str(template).replace('{PRODUCT.METAFIELDS.LENS_TECHNOLOGY}', str(product.metafields.lens_technology).strip().upper()).strip()
            elif '{product.metafields.lens_technology}' in template: template = str(template).replace('{product.metafields.lens_technology}', str(product.metafields.lens_technology).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

            if '{Product.Metafields.Frame_Material}' in template: template = str(template).replace('{Product.Metafields.Frame_Material}', str(product.metafields.frame_material).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.FRAME_MATERIAL}' in template: template = str(template).replace('{PRODUCT.METAFIELDS.FRAME_MATERIAL}', str(product.metafields.frame_material).strip().upper()).strip()
            elif '{product.metafields.frame_material}' in template: template = str(template).replace('{product.metafields.frame_material}', str(product.metafields.frame_material).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()
            
            if '{Product.Metafields.Frame_Shape}' in template: template = str(template).replace('{Product.Metafields.Frame_Shape}', str(product.metafields.frame_shape).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.FRAME_SHAPE}' in template: template = str(template).replace('{PRODUCT.METAFIELDS.FRAME_SHAPE}', str(product.metafields.frame_shape).strip().upper()).strip()
            elif '{product.metafields.frame_shape}' in template: template = str(template).replace('{product.metafields.frame_shape}', str(product.metafields.frame_shape).strip().lower()).strip()
            template = str(template).replace('  ', ' ').strip()

        except Exception as e:
            self.print_logs(f'Exception in get_original_text: {e}')
            if self.DEBUG: print(f'Exception in get_original_text: {e}')
            else: pass
        finally: return template

    # get matched product between database products and shopify products
    def get_matched_product(self, shopify_id, shopify_products) -> dict:
        shopify_product = {}
        try:
            for product in shopify_products:
                if str(product['id']).strip() == shopify_id:
                    shopify_product = product
                    break
        except Exception as e:
            self.print_logs(f'Exception in get_matched_product: {e}')
            if self.DEBUG: print(f'Exception in get_matched_product: {e}')
        finally: return shopify_product

    # check product title and update it if not matched
    def check_product_title(self, new_product_title: str, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product title if shopify product title is not equal to database product title
            if str(new_product_title).strip() != str(shopify_product['title']).strip():
                if not shopify_processor.update_product_title(product.shopify_id, new_product_title):
                    print(f'Failed to update product title\n Old Product Title: {shopify_product["title"]}\nNew Product Title: {new_product_title}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_title: {e}')
            if self.DEBUG: print(f'Excepption in check_product_title: {e}')
            else: pass

    # check product description of shopify product and update it if not matched
    def check_product_description(self, product_description: str, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product status if shopify product status is not equal to database product status
            if str(product_description).strip() != str(shopify_product['body_html']).strip():
                if not shopify_processor.update_product_body_html(product.shopify_id, str(product_description).strip()):
                    print(f'Failed to update product description\n Old Product Description: {shopify_product["body_html"]}\nNew Product Description: {str(product_description).strip()}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_description: {e}')
            if self.DEBUG: print(f'Excepption in check_product_description: {e}')
            else: pass

    # check product meta title and update it if not matched
    def check_product_meta_title(self, meta_title: str, new_product_title: str, product: Product, shopify_metafields: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            if not self.DEBUG and str(meta_title).strip():
                metafield_found_status, metafield_id, shopify_metafield__title_tag = self.get_matched_metafiled(shopify_metafields, 'title_tag')
                if metafield_found_status:
                    if str(shopify_metafield__title_tag).strip().lower() != str(meta_title).strip().lower():
                        old_title_tag = str(shopify_metafield__title_tag).strip().title()
                        new_title_tag = str(meta_title).strip()
                        if not shopify_processor.update_title_tag_metafield(metafield_id, new_title_tag, new_product_title):
                            print(f'Failed to update product meta title\nOld meta title: {old_title_tag}\nNew meta title: {new_title_tag}')
                else:
                    json_metafield = {'namespace': 'global', 'key': 'title_tag', "value": str(meta_title).strip(), "value_type": "string"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
        except Exception as e:
            self.print_logs(f'Exception in check_product_meta_title: {e}')
            if self.DEBUG: print(f'Excepption in check_product_meta_title: {e}')
            else: pass

    # check product meta description and update it if not matched
    def check_product_meta_description(self, meta_description: str, new_product_title: str, product: Product, shopify_metafields: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            if not self.DEBUG and str(meta_description).strip():
                metafield_found_status, metafield_id, shopify_metafield__description_tag = self.get_matched_metafiled(shopify_metafields, 'description_tag')
                if metafield_found_status:
                    if str(shopify_metafield__description_tag).strip() != str(meta_description).strip():
                        old_description_tag = str(shopify_metafield__description_tag).strip().title()
                        new_description_tag = meta_description
                        if not shopify_processor.update_description_tag_metafield(metafield_id, new_description_tag, new_product_title):
                            print(f'Failed to update product meta description\nOld meta description: {old_description_tag}\nNew meta description: {new_description_tag}')
                else:
                    json_metafield = {'namespace': 'global', 'key': 'description_tag', "value": str(meta_description).strip(), "value_type": "string"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
        except Exception as e:
            self.print_logs(f'Exception in check_product_meta_description: {e}')
            if self.DEBUG: print(f'Excepption in check_product_meta_description: {e}')
            else: pass

    # get specific matched metafileds from all metafields of product
    def get_matched_metafiled(self, shopify_metafields: list[dict], metafield_name: str):
        metafield_id = ''
        metafield_value = ''
        metafield_found_status = False
        try:
            for shopify_metafield in shopify_metafields['metafields']:
                if shopify_metafield['key'] == metafield_name:
                    metafield_id = str(shopify_metafield['id']).strip()
                    metafield_value = str(shopify_metafield['value']).strip()
                    metafield_found_status = True
                    break
        except Exception as e:
            self.print_logs(f'Exception in get_matched_metafiled: {e}')
            if self.DEBUG: print(f'Exception in get_matched_metafiled: {e}')
            else: pass
        finally: return metafield_found_status, metafield_id, metafield_value

    def check_product_image_description(self, image_description: str, shopify_product: dict, shopify_processor: Shopify_Processor, product_title: str):
        try:
            if shopify_product['images']:
                for image in shopify_product['images']:
                    image_id = str(image['id']).strip()
                    product_id = str(image['product_id']).strip()
                    if image_description != str(image['alt']):
                        shopify_processor.update_product_image_alt_text(product_id, image_id, image_description, product_title)
            else:
                if shopify_product['image']:
                    image = shopify_product['image']
                    image_id = str(image['id']).strip()
                    product_id = str(image['product_id']).strip()
                    if image_description != str(image['alt']):
                        shopify_processor.update_product_image_alt_text(product_id, image_id, image_description, product_title)
        except Exception as e:
            self.print_logs(f'Exception in check_product_image_description: {e}')
            if self.DEBUG: print(f'Excepption in check_product_image_description: {e}')
            else: pass

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass


class Shopify_Controller:
    def __init__(self, DEBUG: bool, path: str) -> None:
        self.DEBUG = DEBUG
        self.connection_file = f'{path}/files/conn.txt'
        self.config_file: str = f'{path}/files/config.json'
        self.logs_folder_path: str = f'{path}/Logs/Shopify Updater/'
        self.logs_filename: str = ''
        pass

    def controller(self):
        try:
            if not os.path.exists('Logs'): os.makedirs('Logs')
            if not os.path.exists(self.logs_folder_path): os.makedirs(self.logs_folder_path)
            if not self.logs_filename: self.create_logs_filename()

            fields_to_update = self.get_update_fields()
            if fields_to_update:
                
                # reading connection file
                file_reader = Files_Reader(self.DEBUG)
                conn_string = file_reader.read_text_file(self.connection_file)

                # getting all stores from database
                query_processor = Query_Processor(self.DEBUG, conn_string)
                brands = query_processor.get_brands()

                selected_brands = self.get_brands_to_update(brands)

                if selected_brands:
                    for selected_brand in selected_brands:
                        product_types = query_processor.get_product_types_by_store_id(selected_brand.store_id)
                        selected_product_types = self.get_product_type_to_update(selected_brand, product_types)
                        
                        if selected_product_types:
                            
                            for selected_product_type in selected_product_types:
                                products: list[Product] = []
                                condition = f'type="{selected_product_type}"'
                                products = query_processor.get_products_by_brand_id(selected_brand.id, condition)
                                variants = query_processor.get_variants_by_brand_id(selected_brand.id)
                                metafields = query_processor.get_metafields_by_brand_id(selected_brand.id)
                                for product in products:
                                    self.get_variants_and_metafileds_for_product(product, variants, metafields)
                                    selected_brand.products = product
                            
                        else: print(f'No product type selected for {selected_brand.name}')


                    for selected_brand in selected_brands:
                        store = query_processor.get_store_by_id(selected_brand.store_id)
                        Shopify_Updater(self.DEBUG, self.config_file, store, self.logs_filename).controller(fields_to_update, selected_brand)

                else: print(f'No brand selected')
            else: print('No field selected to update')
        except Exception as e:
            if self.DEBUG: print(f'Exception in Shopify_Controller controller: {e}')
            self.print_logs(f'Exception in Shopify_Controller controller: {str(e)}')

    # create logs filename
    def create_logs_filename(self) -> None:
        try:
            scrape_time = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
            self.logs_filename = f'{self.logs_folder_path}Logs {scrape_time}.txt'
        except Exception as e:
            self.print_logs(f'Exception in create_logs_filename: {str(e)}')
            if self.DEBUG: print(f'Exception in create_logs_filename: {e}')

    def get_update_fields(self):
        update_fields = []
        try:
            fields = ['Product Title', 'Product Description', 'Meta Title', 'Meta Description', 'Image Description']
            print('\nSelect fields to update:')
            for field_index, field in enumerate(fields):
                print(field_index + 1, field)

            while True:
                choices = ''
                try:
                    choices = input('Choice: ')
                    if choices:
                        for choice in choices.split(','):
                            update_fields.append(fields[int(str(choice).strip()) - 1])
                        break
                    else: print(f'Please enter number from 1 to {len(fields)}')
                except Exception as e:
                    print(f'Please enter number from 1 to {len(fields)}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_update_fields: {e}')
            self.print_logs(f'Exception in get_update_fields: {str(e)}')
        finally: return update_fields

    def get_brands_to_update(self, brands: list[Brand]) -> list[Brand]:
        selected_brands = []
        try:
            print('\nSelect brands:')
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
            self.print_logs(f'Exception in get_brands_to_update: {str(e)}')
        finally: return selected_brands

    def get_product_type_to_update(self, brand: Brand, product_types: list[str]) -> list[str]:
        selected_product_types = []
        try:
            print(f'\nSelect product type for {brand.name}:')
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
            self.print_logs(f'Exception in get_product_type_to_update: {str(e)}')
        finally: return selected_product_types

    def get_variants_and_metafileds_for_product(self, product: Product, variants: list[Variant], metafields: list[Metafields]):
        try:
            for variant in variants:
                if variant.product_id == product.id:
                    product.variants = variant

            for metafield in metafields:
                if metafield.product_id == product.id:
                    product.metafields = metafield
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_variants_and_metafileds_for_product: {e}')
            self.print_logs(f'Exception in get_variants_and_metafileds_for_product: {str(e)}')

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass


DEBUG = True
try:
    pathofpyfolder = os.path.realpath(sys.argv[0])
    # get path of Exe folder
    path = pathofpyfolder.replace(pathofpyfolder.split('\\')[-1], '')
    
    Shopify_Controller(DEBUG, path).controller()

except Exception as e:
    if DEBUG: print('Exception: '+str(e))
    else: pass


