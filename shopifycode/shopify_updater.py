import os
# import json
import base64
# from PIL import Image
# from time import sleep
# from urllib.parse import quote

from models.store import Store
from models.brand import Brand
from models.product import Product
from models.variant import Variant
# from models.metafields import Metafields

from modules.files_reader import Files_Reader
from modules.query_processor import Query_Processor

from shopifycode.shopify_processor import Shopify_Processor

class Shopify_Updater:
    def __init__(self, DEBUG: bool, store: Store, config_file: str, query_processor: Query_Processor, logs_filename: str) -> None:
        self.DEBUG: bool = DEBUG
        self.store: Store = store
        self.config_file: str = config_file
        self.template_file_path = ''
        self.new_products: list[Product] = []
        self.new_variants: list[Variant] = []
        self.updated_variants: list[Variant] = []
        self.not_found_variants: list[Variant] = []
        self.query_processor = query_processor
        self.logs_filename = logs_filename
        pass

    def controller(self, brands: list[Brand]) -> None:
        try:
            self.get_templates_folder_path()
            print(self.template_file_path)
            shopify_processor = Shopify_Processor(self.DEBUG, self.config_file, self.logs_filename)
            shopify_processor.get_store_url()
            
            print(f'Updating Shopify for {str(self.store.name).strip().title()}')

            for brand in brands:
                print(f'Brand: {brand.name}')
                print(f'No. of Products in database: {len(brand.products)}')

                products_count = shopify_processor.get_count_of_products_by_vendor(brand.name)
                print(f'No. of Products in shopify: {products_count}')

                self.printProgressBar(0, len(brand.products), prefix = 'Progress:', suffix = 'Complete', length = 50)
                shopify_products = shopify_processor.get_products_by_vendor(brand.name)

                if products_count == len(shopify_products):
                    for product_index, product in enumerate(brand.products):
                        # get product title
                        new_product_title = self.create_product_title(brand, product)

                        if product.shopify_id:
                            # get matched product between shopify products nad dataabase product
                            shopify_product = self.get_matched_product(product.shopify_id, shopify_products)

                            if shopify_product and 'Outlet' not in shopify_product['tags']: 
                                # if self.DEBUG: print(new_product_title)

                                self.check_product_title(new_product_title, product, shopify_product, shopify_processor)
                                self.check_product_description(brand, product, shopify_product, shopify_processor)
                                self.check_product_status(product, shopify_product, shopify_processor)
                                self.check_product_type(product, shopify_product, shopify_processor)
                                self.check_product_tags(brand, product, shopify_product, shopify_processor)

                                image_description = self.create_product_image_description(brand, product)
                                # check if database product has 360 images for product
                                if product.metafields.img_360_urls:
                                    # check if the total number of 360 images of shopify product is less than total number of 360 images in database
                                    # if len(shopify_product['images']) < len(product.metafields.img_360_urls):
                                    #     self.add_product_360_images(shopify_product, product, new_product_title, shopify_processor)
                                        
                                    # self.check_product_360_images_tag(product, shopify_product, shopify_processor)
                                    if len(shopify_product['images']) == 0: self.add_product_360_images(product, image_description, shopify_processor)
                                    self.check_product_360_images_tag(product, shopify_product, shopify_processor)
                                elif not shopify_product['image'] and str(product.metafields.img_url).strip():
                                    self.add_product_image(product, new_product_title, shopify_processor)
                                
                                if image_description: 
                                    self.check_product_images_alt_text(image_description, new_product_title, shopify_product, shopify_processor)

                                self.check_product_options(product, shopify_product, shopify_processor)

                                shopify_metafields = shopify_processor.get_product_metafields_from_shopify(product.shopify_id)
                                if shopify_metafields:
                                    self.check_product_metafields(new_product_title, brand, product, shopify_metafields, shopify_processor)
                                else:
                                    new_metafields = self.get_new_product_metafeilds(product)
                                    for new_metafield in new_metafields: 
                                        shopify_processor.set_metafields_for_product(product.shopify_id, new_metafield)
                                        
                                for variant in product.variants:
                                    if variant.shopify_id: self.check_product_variant(new_product_title, variant, product, shopify_product, shopify_processor)
                                    else: 
                                        self.add_new_variant(variant, product, new_product_title, shopify_processor)
                            else: 
                                if shopify_product: 
                                    self.print_logs(f'Outlet tag found for {new_product_title}')
                                else:
                                    # this product is deleted from the store
                                    self.print_logs(f'{new_product_title} product not found on shopify store')
                        
                        else:
                            self.add_new_product(new_product_title, product, brand, shopify_processor)

                        self.printProgressBar(product_index + 1, len(brand.products), prefix = 'Progress:', suffix = 'Complete', length = 50)
        
        except Exception as e:
            self.print_logs(f'Exception in Shopify_Updater controller: {e}')
            if self.DEBUG: print(f'Exception in Shopify_Updater controller: {e}')
            else: pass

    def get_templates_folder_path(self) -> None:
        try:
            if self.store.id == 1: self.template_file_path = 'templates/Digitalhub/'
            elif self.store.id == 2: self.template_file_path = 'templates/Safilo/'
            elif self.store.id == 3: self.template_file_path = 'templates/Keringeyewear/'
            elif self.store.id == 4: self.template_file_path = 'templates/Rudyproject/'
            elif self.store.id == 5: self.template_file_path = 'templates/Luxottica/'
        except Exception as e:
            self.print_logs(f'Exception in get_templates_folder_path: {e}')
            if self.DEBUG: print(f'Exception in get_templates_folder_path: {e}')
            else: pass

    # create product title
    def create_product_title(self, brand: Brand, product: Product) -> str:
        title = ''
        try:
            title_template_path = self.get_template_path('Product Title', brand, product)
            title_template = self.get_template(title_template_path)
            if title_template:
                title = self.get_original_text(title_template, brand, product)
            
            else:
                if str(brand.name).strip(): title += f'{str(brand.name).strip().title()}'
                if str(product.name).strip(): title += f' {str(product.name).strip().upper()}'
                if str(product.number).strip(): title += f' {str(product.number).strip().upper()}'
                if str(product.frame_code).strip(): title += f' {str(product.frame_code).strip().upper()}'
                # if str(product.frame_code).strip(): title += f'{str(product.frame_code).strip().upper()} - '
                # if str(product.frame_color).strip(): title += f'{str(product.frame_color).strip().upper()} - '
                # if str(product.lens_color).strip(): title += f'{str(product.lens_color).strip().title()} '

                title = str(title).strip()
                if '  ' in title: title = str(title).strip().replace('  ', ' ')
                if str(title).strip()[-1] == '-': title = str(title)[:-1].strip()
        except Exception as e:
            self.print_logs(f'Exception in create_product_title: {e}')
            if self.DEBUG: print(f'Exception in create_product_title: {e}')
            else: pass
        finally: return title

    # create product description
    def create_product_description(self, brand: Brand, product: Product) -> str:
        product_description = ''
        try:
            product_description_template_path = self.get_template_path('Product Description', brand, product)
            product_description_template = self.get_template(product_description_template_path)
            product_description = self.get_original_text(product_description_template, brand, product)
        except Exception as e:
            self.print_logs(f'Exception in create_product_description: {e}')
            if self.DEBUG: print(f'Exception in create_product_description: {e}')
        finally: return product_description

    # create product meta title
    def create_product_meta_title(self, brand: Brand, product: Product) -> str:
        meta_title = ''
        try:
            meta_title_template_path = self.get_template_path('Meta Title', brand, product)
            meta_title_template = self.get_template(meta_title_template_path)
            meta_title = self.get_original_text(meta_title_template, brand, product)

            if meta_title:
                meta_title = str(meta_title).replace('  ', ' ').strip()
                if len(meta_title) > 60: meta_title = str(meta_title).replace('| LookerOnline', '| LO')
        except Exception as e:
            self.print_logs(f'Exception in create_product_meta_title: {e}')
            if self.DEBUG: print(f'Exception in create_product_meta_title: {e}')
        finally: return meta_title

    # create product meta description
    def create_product_meta_description(self, brand: Brand, product: Product) -> str:
        meta_description = ''
        try:
            meta_description_template_path = self.get_template_path('Meta Description', brand, product)
            meta_description_template = self.get_template(meta_description_template_path)
            meta_description = self.get_original_text(meta_description_template, brand, product)

            if meta_description:
                meta_description = str(meta_description).replace('  ', ' ').replace('âœ“', '✓').strip()
        except Exception as e:
            self.print_logs(f'Exception in create_product_meta_description: {e}')
            if self.DEBUG: print(f'Exception in create_product_meta_description: {e}')
        finally: return meta_description

    # create product image description
    def create_product_image_description(self, brand: Brand, product: Product) -> str:
        image_description = ''
        try:
            image_description_template_path = self.get_template_path('Image Description', brand, product)
            image_description_template = self.get_template(image_description_template_path)
            image_description = self.get_original_text(image_description_template, brand, product)
        except Exception as e:
            self.print_logs(f'Exception in create_product_image_description: {e}')
            if self.DEBUG: print(f'Exception in create_product_image_description: {e}')
        finally: return image_description

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
            else: pass
        finally: return template
    
    # get original text from template
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
            if '{Product.Metafields.For_Who}' in template: 
                if 'unisex' in str(product.metafields.for_who).strip().lower(): template = str(template).replace('{Product.Metafields.For_Who}', 'MEN and WOMEN').strip()
                else: template = str(template).replace('{Product.Metafields.For_Who}', str(product.metafields.for_who).strip().title()).strip()
            elif '{PRODUCT.METAFIELDS.FOR_WHO}' in template:
                if 'unisex' in str(product.metafields.for_who).strip().lower(): template = str(template).replace('{Product.Metafields.For_Who}', 'Men and Women').strip() 
                else: template = str(template).replace('{PRODUCT.METAFIELDS.FOR_WHO}', str(product.metafields.for_who).strip().upper()).strip()
            elif '{product.metafields.for_who}' in template:
                if 'unisex' in str(product.metafields.for_who).strip().lower(): template = str(template).replace('{Product.Metafields.For_Who}', 'men and women').strip()
                else: template = str(template).replace('{product.metafields.for_who}', str(product.metafields.for_who).strip().lower()).strip()
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
            else: pass
        finally: return shopify_product

    # check product title and update it if not matched
    def check_product_title(self, new_product_title: str, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product title if shopify product title is not equal to database product title
            if str(new_product_title).strip() != str(shopify_product['title']).strip():
                product_json = { "product": { "id": product.shopify_id, "title": str(new_product_title).strip() } }
                if not shopify_processor.update_product(product.shopify_id, product_json):
                    self.print_logs(f'Failed to update product title\n Old Product Title: {shopify_product["title"]}\nNew Product Title: {new_product_title}')
            # if str(new_product_title).strip() != str(shopify_product['title']).strip():
            #     if not shopify_processor.update_product_title(product.shopify_id, new_product_title):
            #         self.print_logs(f'Failed to update product title\n Old Product Title: {shopify_product["title"]}\nNew Product Title: {new_product_title}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_title: {e}')
            if self.DEBUG: print(f'Excepption in check_product_title: {e}')
            else: pass

    # check product description of shopify product and update it if not matched
    def check_product_description(self, brand: Brand, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            product_description_template_path = self.get_template_path('Product Description', brand, product)
            product_description_template = self.get_template(product_description_template_path)
            if product_description_template:
                product_description = self.get_original_text(product_description_template, brand, product)

                # update product status if shopify product status is not equal to database product status
                if str(product_description).strip() != str(shopify_product['body_html']).strip():
                    product_json = { "product": { "id": product.shopify_id, "body_html": str(product_description).strip() } }
                    if not shopify_processor.update_product(product.shopify_id, product_json):
                        self.print_logs(f'Failed to update product description\n Old Product Description: {shopify_product["body_html"]}\nNew Product Description: {str(product_description).strip()}')
                # if str(product_description).strip() != str(shopify_product['body_html']).strip():
                #     if not shopify_processor.update_product_body_html(product.shopify_id, str(product_description).strip()):
                #         self.print_logs(f'Failed to update product description\n Old Product Description: {shopify_product["body_html"]}\nNew Product Description: {str(product_description).strip()}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_description: {e}')
            if self.DEBUG: print(f'Excepption in check_product_description: {e}')
            else: pass

    # check product status of shopify product and update it if not matched
    def check_product_status(self, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product status if shopify product status is not equal to database product status
            if str(product.status).strip() != str(shopify_product['status']).strip():
                product_json = { "product": { "id": product.shopify_id, "status": str(product.status).strip() } }
                if not shopify_processor.update_product(product.shopify_id, product_json):
                    self.print_logs(f'Failed to update product status\n Old Product Status: {shopify_product["status"]}\nNew Product Status: {str(product.status).strip()}')
            # if str(product.status).strip() != str(shopify_product['status']).strip():
            #     if not shopify_processor.update_product_status(product.shopify_id, str(product.status).strip()):
            #         self.print_logs(f'Failed to update product status\n Old Product Status: {shopify_product["status"]}\nNew Product Status: {str(product.status).strip()}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_status: {e}')
            if self.DEBUG: print(f'Excepption in check_product_status: {e}')
            else: pass

    # check product type of shopify product and update it if not matched
    def check_product_type(self, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product type if shopify product type is not equal to database product type
            if str(product.type).strip() != str(shopify_product['product_type']).strip():
                product_json = { "product": { "id": product.shopify_id, "product_type": str(product.type).strip() } }
                if not shopify_processor.update_product(product.shopify_id, product_json):
                    self.print_logs(f'Failed to update product type\n Old Product Type: {shopify_product["product_type"]}\nNew Product Type: {str(product.type).strip()}')
            # if str(product.type).strip() != str(shopify_product['product_type']).strip():
            #     if not shopify_processor.update_product_type(product.shopify_id, str(product.type).strip()):
            #         self.print_logs(f'Failed to update product type\n Old Product Type: {shopify_product["product_type"]}\nNew Product Type: {str(product.type).strip()}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_type: {e}')
            if self.DEBUG: print(f'Excepption in check_product_type: {e}')
            else: pass

    # check product tags of shopify product and update them if not matched
    def check_product_tags(self, brand: Brand, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            # update product tags
            shopify_product_tags_list = str(shopify_product['tags']).strip().split(', ')
            tags = ', '.join(self.get_product_tags(brand, product, shopify_product_tags_list))
            if tags:
                new_tags = f"{str(shopify_product['tags']).strip()}, {tags}"
                if not shopify_processor.update_product_tags(product.shopify_id, new_tags):
                    self.print_logs(f'Failed to update product tags\n Old Product Tags: {shopify_product["tags"]}\nNew Product Tags: {new_tags}')
        except Exception as e:
            self.print_logs(f'Exception in check_product_tags: {e}')
            if self.DEBUG: print(f'Excepption in check_product_tags: {e}')
            else: pass

    # get product tags whcih are not on shopify
    def get_product_tags(self, brand: Brand, product: Product, shopify_product_tags: list[str]) -> list[str]:
        tags = []
        try:
            if str(brand.name).strip() and str(brand.name).strip() not in shopify_product_tags: tags.append(str(brand.name).strip())
            if str(product.number).strip() and str(product.number).strip().upper() not in shopify_product_tags: tags.append(str(product.number).strip().upper())
            if str(product.name).strip() and str(product.name).strip().upper() not in shopify_product_tags: tags.append(str(product.name).strip().upper())
            if str(product.frame_code).strip() and str(product.frame_code).strip().upper() not in shopify_product_tags: tags.append(str(product.frame_code).strip().upper())
            if str(product.lens_code).strip() and str(product.lens_code).strip().upper() not in shopify_product_tags: tags.append(str(product.lens_code).strip().upper())
            if str(product.type).strip() and str(product.type).strip() not in shopify_product_tags: tags.append(str(product.type).strip())
            if str(product.metafields.for_who).strip():
                if str(product.metafields.for_who).strip().lower() == 'unisex':
                    if 'Men'  not in shopify_product_tags: tags.append('Men')
                    if 'Women'  not in shopify_product_tags: tags.append('Women')
                else:
                    if str(product.metafields.for_who).strip() not in shopify_product_tags: 
                        tags.append(str(product.metafields.for_who).strip())
            if str(product.metafields.activity).strip() and str(product.metafields.activity).strip() not in shopify_product_tags: tags.append(str(product.metafields.activity).strip())
            if str(product.metafields.lens_material).strip() and str(product.metafields.lens_material).strip() not in shopify_product_tags: tags.append(str(product.metafields.lens_material).strip())
            if str(product.metafields.graduabile) and str(product.metafields.graduabile) not in shopify_product_tags: tags.append(str(product.metafields.graduabile).strip())
            if str(product.metafields.interest) and str(product.metafields.interest) not in shopify_product_tags: tags.append(str(product.metafields.interest).strip())
            if str(product.metafields.lens_technology).strip() and str(product.metafields.lens_technology).strip() not in shopify_product_tags: tags.append(str(product.metafields.lens_technology).strip())
            if str(product.metafields.frame_shape).strip() and str(product.metafields.frame_shape).strip() not in shopify_product_tags: tags.append(str(product.metafields.frame_shape).strip())
            if str(product.metafields.frame_material).strip() and str(product.metafields.frame_material).strip() not in shopify_product_tags: tags.append(str(product.metafields.frame_material).strip())
        except Exception as e:
            self.print_logs(f'Exception in get_product_tags: {e}')
            if self.DEBUG: print(f'Exception in get_product_tags: {e}')
            else: pass
        finally: return tags

    # add 360 images to the product on shopify
    def add_product_360_images(self, product: Product, image_description: str, shopify_processor: Shopify_Processor) -> None:
        try:
            if self.store.id == 1:
                img_360_urls = product.metafields.img_360_urls

                if '_08.' in img_360_urls[-1]:
                    last_image = img_360_urls.pop(-1)
                    img_360_urls.insert(0, last_image)

                for index, image_360_url in enumerate(img_360_urls):
                    image_filename = ''
                    image_filename = str(image_360_url).strip().split('/')[-1].strip()
                    if image_filename:
                        image_attachment = shopify_processor.download_image(image_360_url)
                        if image_attachment:
                            # save downloaded image
                            with open(image_filename, 'wb') as f: f.write(image_attachment)
                            # crop image to the correct size
                            shopify_processor.crop_downloaded_image(image_filename)
                            # open croped image
                            f = open(image_filename, 'rb')
                            image_attachment = base64.b64encode(f.read())
                            f.close()

                            json_value = {"image": {"position": index + 1, "attachment": image_attachment.decode('utf-8'), "filename": image_filename, "alt": image_description}}
                            shopify_processor.update_product_image(product.shopify_id, json_value)
                            # delete downloaded image
                            os.remove(image_filename)
            elif self.store.id == 2:
                for index, image_360_url in enumerate(product.metafields.img_360_urls):
                    image_filename = ''
                    image_360_url = str(image_360_url).strip()
                    image_filename = f'{str(image_description).replace(" ", "_")}__{index + 1}.png'
                    if image_filename:
                        json_value = {"image": {"position": index + 1, "src": image_360_url, "filename": image_filename, "alt": image_description}}
                        shopify_processor.update_product_image(product.shopify_id, json_value)
            
            elif self.store.id == 5:
                for index, image_360_url in enumerate(product.metafields.img_360_urls):
                    image_filename = ''
                    image_360_url = str(image_360_url).strip()
                    if '?impolicy=MYL_EYE&wid=688' not in image_360_url:
                        image_360_url = str(image_360_url).replace('?impolicy=MYL_EYE&wid=688', '').strip()
                        image_filename = image_360_url.split('/')[-1].strip()

                        if '?' in image_filename: image_filename = str(image_filename).split('?')[0].strip()
                        if image_filename[0] == '0': image_filename = image_filename[1:]

                        if image_filename:
                            json_value = {"image": {"position": index + 1, "src": image_360_url, "filename": image_filename, "alt": image_description}}
                            shopify_processor.update_product_image(product.shopify_id, json_value)
        except Exception as e:
            self.print_logs(f'Exception in add_product_360_images: {e}')
            if self.DEBUG: print(f'Excepption in add_product_360_images: {e}')
            else: pass

    # check image 360 tag on the shopify product and if not found add it
    def check_product_360_images_tag(self, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            again_shopify_product = shopify_processor.get_product_from_shopify(shopify_product['id'])
            spin_images = len(again_shopify_product['product']['images'])
            if int(spin_images) > 1 and f'spinimages={spin_images}' not in str(again_shopify_product['product']['tags']).strip():
                tags = f"{again_shopify_product['product']['tags']}, spinimages={spin_images}"
                shopify_processor.update_product_tags(product.shopify_id, tags)
        except Exception as e:
            self.print_logs(f'Exception in check_product_360_images_tag: {e}')
            if self.DEBUG: print(f'Excepption in check_product_360_images_tag: {e}')
            else: pass
        
    # add product image to the shopify
    def add_product_image(self, product: Product, image_description: str, shopify_processor: Shopify_Processor) -> None:
        try:
            image_attachment = shopify_processor.download_image(str(product.metafields.img_url).strip())
            if image_attachment:
                image_attachment = base64.b64encode(image_attachment)
                image_filename = str(product.metafields.img_url).strip().split('/')[-1].strip()
                json_value = {"image": {"position": 1, "attachment": image_attachment.decode('utf-8'), "filename": image_filename, "alt": image_description}}
                if not shopify_processor.update_product_image(product.shopify_id, json_value):
                    self.print_logs(f'Failed to update product: {product.shopify_id} image')
            else: self.print_logs(f'Failed to download image for {product.number} {product.frame_code}')
        except Exception as e:
            self.print_logs(f'Exception in add_product_image: {e}')
            if self.DEBUG: print(f'Excepption in add_product_image: {e}')
            else: pass
    
    # check alt text of images 360 on the shopify product and if not found add it
    def check_product_images_alt_text(self, image_description: str, new_product_title: str, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            if image_description:
                if shopify_product['images']:
                    for image in shopify_product['images']:
                        image_id = str(image['id']).strip()
                        product_id = str(image['product_id']).strip()
                        if image_description != str(image['alt']):
                            shopify_processor.update_product_image_alt_text(product_id, image_id, image_description, new_product_title)
                else:
                    if shopify_product['image']:
                        image = shopify_product['image']
                        image_id = str(image['id']).strip()
                        product_id = str(image['product_id']).strip()
                        if image_description != str(image['alt']):
                            shopify_processor.update_product_image_alt_text(product_id, image_id, image_description, new_product_title)
        except Exception as e: 
            self.print_logs(f'Excepption in check_product_images_alt_text: {e}')
            if self.DEBUG: print(f'Excepption in check_product_images_alt_text: {e}')
            else: pass
            
    # check product options and modify them if needed
    def check_product_options(self, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            if len(product.variants) > 1 and shopify_product['options']:
                for option in shopify_product['options']:
                    if option['name'] == 'Title':
                        shopify_processor.update_product_options(option['product_id'], option['id'], 'Size')
        except Exception as e:
            self.print_logs(f'Exception in check_product_options: {e}')
            if self.DEBUG: print(f'Excepption in check_product_options: {e}')
            else: pass

    # check product metafields of database with shopify
    def check_product_metafields(self, new_product_title: str, brand: Brand, product: Product, shopify_metafields: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            title_tag = self.create_product_meta_title(brand, product)
            description_tag = self.create_product_meta_description(brand, product)

            if str(product.metafields.for_who).strip():
                metafield_found_status, metafield_id, shopify_metafield__for_who = self.get_matched_metafiled(shopify_metafields, 'for_who')
                if metafield_found_status:
                    if str(shopify_metafield__for_who).strip().title() != str(product.metafields.for_who).strip().title():
                        old_for_who = str(shopify_metafield__for_who).strip().title()
                        new_for_who = str(product.metafields.for_who).strip().title()
                        if not shopify_processor.update_for_who_metafield(metafield_id, new_for_who, new_product_title):
                            self.print_logs(f'Failed to update product gender metafield\nOld gender metafield: {old_for_who}\nNew gender metafield: {new_for_who}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "for_who", "value": str(product.metafields.for_who).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.frame_color).strip():
                metafield_found_status, metafield_id, shopify_metafield__frame_color = self.get_matched_metafiled(shopify_metafields, 'frame_color')
                if metafield_found_status:
                    if str(shopify_metafield__frame_color).strip().title() != str(product.frame_color).strip().title():
                        old_frame_color = str(shopify_metafield__frame_color).strip().title()
                        new_frame_color = str(product.frame_color).strip().title()
                        if not shopify_processor.update_frame_color_metafield(metafield_id, new_frame_color, new_product_title):
                            self.print_logs(f'Failed to update product frame color metafield\nOld frame color metafield: {old_frame_color}\nNew frame color metafield: {new_frame_color}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "frame_color", "value": str(product.frame_color).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.frame_material).strip():
                metafield_found_status, metafield_id, shopify_metafield__frame_material = self.get_matched_metafiled(shopify_metafields, 'frame_material')
                if metafield_found_status:
                    if str(shopify_metafield__frame_material).strip().title() != str(product.metafields.frame_material).strip().title():
                        old_frame_material = str(shopify_metafield__frame_material).strip().title()
                        new_frame_material = str(product.metafields.frame_material).strip().title()
                        if not shopify_processor.update_frame_material_metafield(metafield_id, new_frame_material, new_product_title):
                            self.print_logs(f'Failed to update product frame material metafield\nOld frame material metafield: {old_frame_material}\nNew frame material metafield: {new_frame_material}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "frame_material", "value": str(product.metafields.frame_material).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.frame_shape).strip():
                metafield_found_status, metafield_id, shopify_metafield__frame_shape = self.get_matched_metafiled(shopify_metafields, 'frame_shape')
                if metafield_found_status:
                    if str(shopify_metafield__frame_shape).strip().title() != str(product.metafields.frame_shape).strip().title():
                        old_frame_shape = str(shopify_metafield__frame_shape).strip().title()
                        new_frame_shape = str(product.metafields.frame_shape).strip().title()
                        if not shopify_processor.update_frame_shape_metafield(metafield_id, new_frame_shape, new_product_title):
                            self.print_logs(f'Failed to update product frame shape metafield\nOld frame shape metafield: {old_frame_shape}\nNew frame_shape metafield: {new_frame_shape}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "frame_shape", "value": str(product.metafields.frame_shape).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.lens_color).strip():
                metafield_found_status, metafield_id, shopify_metafield__lens_color = self.get_matched_metafiled(shopify_metafields, 'lens_color')
                if metafield_found_status:
                    if str(product.lens_color).strip() and str(shopify_metafield__lens_color).strip().title() != str(product.lens_color).strip().title():
                        old_lens_color = str(shopify_metafield__lens_color).strip().title()
                        new_lens_color = str(product.lens_color).strip().title()
                        if not shopify_processor.update_lens_color_metafield(metafield_id, new_lens_color, new_product_title):
                            self.print_logs(f'Failed to update product lens color metafield\nOld lens color metafield: {old_lens_color}\nNew lens color metafield: {new_lens_color}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "lens_color", "value": str(product.lens_color).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.lens_technology).strip():
                metafield_found_status, metafield_id, shopify_metafield__lens_technology = self.get_matched_metafiled(shopify_metafields, 'lens_technology')
                if metafield_found_status:
                    if str(product.metafields.lens_technology).strip() and str(shopify_metafield__lens_technology).strip().title() != str(product.metafields.lens_technology).strip().title():
                        old_lens_technology = str(shopify_metafield__lens_technology).strip().title()
                        new_lens_technology = str(product.metafields.lens_technology).strip().title()
                        if not shopify_processor.update_lens_technology_metafield(metafield_id, new_lens_technology, new_product_title):
                            self.print_logs(f'Failed to update product lens technology metafield\nOld lens technology metafield: {old_lens_technology}\nNew lens technology metafield: {new_lens_technology}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "lens_technology", "value": str(product.metafields.lens_technology).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.lens_material).strip():
                metafield_found_status, metafield_id, shopify_metafield__lens_material = self.get_matched_metafiled(shopify_metafields, 'lens_material')
                if metafield_found_status:
                    if str(shopify_metafield__lens_material).strip().title() != str(product.metafields.lens_material).strip().title():
                        old_lens_material = str(shopify_metafield__lens_material).strip().title()
                        new_lens_material = str(product.metafields.lens_material).strip().title()
                        if not shopify_processor.update_lens_material_metafield(metafield_id, new_lens_material, new_product_title):
                            self.print_logs(f'Failed to update product lens material metafield\nOld lens material metafield: {old_lens_material}\nNew lens material metafield: {new_lens_material}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "lens_material", "value": str(product.metafields.lens_material).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.product_size).strip():
                metafield_found_status, metafield_id, shopify_metafield__product_size = self.get_matched_metafiled(shopify_metafields, 'product_size')
                if metafield_found_status:
                    if str(shopify_metafield__product_size).strip() != str(product.metafields.product_size).strip():
                        old_product_size = str(shopify_metafield__product_size).strip().title()
                        new_product_size = str(product.metafields.product_size).strip().title()
                        if not shopify_processor.update_gtin1_metafield(metafield_id, new_product_size, new_product_title):
                            self.print_logs(f'Failed to update product product size metafield\nOld product size metafield: {old_product_size}\nNew product size metafield: {new_product_size}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "product_size", "value": str(product.metafields.product_size).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.gtin1).strip():
                metafield_found_status, metafield_id, shopify_metafield__gtin1 = self.get_matched_metafiled(shopify_metafields, 'gtin1')
                if metafield_found_status:
                    if str(shopify_metafield__gtin1).strip() != str(product.metafields.gtin1).strip():
                        old_gtin1 = str(shopify_metafield__gtin1).strip().title()
                        new_gtin1 = str(product.metafields.gtin1).strip().title()
                        if not shopify_processor.update_gtin1_metafield(metafield_id, new_gtin1, new_product_title):
                            self.print_logs(f'Failed to update product gtin metafield\nOld gtin metafield: {old_gtin1}\nNew gtin metafield: {new_gtin1}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "gtin1", "value": str(product.metafields.gtin1).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.graduabile).strip():
                metafield_found_status, metafield_id, shopify_metafield__graduabile = self.get_matched_metafiled(shopify_metafields, 'graduabile')
                if metafield_found_status:
                    if str(shopify_metafield__graduabile).strip() != str(product.metafields.graduabile).strip().title():
                        old_graduabile = str(shopify_metafield__graduabile).strip().title()
                        new_graduabile = str(product.metafields.graduabile).strip().title()
                        if not shopify_processor.update_graduabile_metafield(metafield_id, new_graduabile, new_product_title):
                            self.print_logs(f'Failed to update product graduabile metafield\nOld graduabile metafield: {old_graduabile}\nNew graduabile metafield: {new_graduabile}')
                else:
                    json_metafield = {"namespace": "my_fields", "key": "graduabile", "value": str(product.metafields.graduabile).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(product.metafields.interest).strip():
                metafield_found_status, metafield_id, shopify_metafield__interest = self.get_matched_metafiled(shopify_metafields, 'interest')
                if metafield_found_status:
                    if str(shopify_metafield__interest).strip() != str(product.metafields.interest).strip().title():
                        old_interest = str(shopify_metafield__interest).strip().title()
                        new_interest = str(product.metafields.interest).strip().title()
                        if not shopify_processor.update_interest_metafield(metafield_id, new_interest, new_product_title):
                            self.print_logs(f'Failed to update product interest metafield\nOld interest metafield: {old_interest}\nNew interest metafield: {new_interest}')
                else:
                    json_metafield = {'namespace': 'my_fields', 'key': 'interest', "value": str(product.metafields.interest).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(title_tag).strip():
                metafield_found_status, metafield_id, shopify_metafield__title_tag = self.get_matched_metafiled(shopify_metafields, 'title_tag')
                if metafield_found_status:
                    if str(shopify_metafield__title_tag).strip() != str(title_tag).strip():
                        old_title_tag = str(shopify_metafield__title_tag).strip().title()
                        new_title_tag = str(title_tag).strip()
                        if not shopify_processor.update_title_tag_metafield(metafield_id, new_title_tag, new_product_title):
                            self.print_logs(f'Failed to update product meta title\nOld meta title: {old_title_tag}\nNew meta title: {new_title_tag}')
                else:
                    json_metafield = {'namespace': 'global', 'key': 'title_tag', "value": str(title_tag).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
            if str(description_tag).strip():
                metafield_found_status, metafield_id, shopify_metafield__description_tag = self.get_matched_metafiled(shopify_metafields, 'description_tag')
                if metafield_found_status:
                    if str(shopify_metafield__description_tag).strip() != str(description_tag).strip():
                        old_description_tag = str(shopify_metafield__description_tag).strip().title()
                        new_description_tag = description_tag
                        if not shopify_processor.update_description_tag_metafield(metafield_id, new_description_tag, new_product_title):
                            self.print_logs(f'Failed to update product meta description\nOld meta description: {old_description_tag}\nNew meta description: {new_description_tag}')
                else:
                    json_metafield = {'namespace': 'global', 'key': 'description_tag', "value": str(description_tag).strip(), "type": "single_line_text_field"}
                    shopify_processor.set_metafields_for_product(product.shopify_id, json_metafield)
        
        except Exception as e: 
            self.print_logs(f'Exception in check_product_metafields: {e}')
            if self.DEBUG: print(f'Exception in check_product_metafields: {e}')
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
        
    # get new product metafields for the product
    def get_new_product_metafeilds(self, brand: Brand, product: Product) -> list[dict]:
        metafields = []
        try:
            meta_title = self.create_product_meta_title(brand, product)
            meta_description = self.create_product_meta_description(brand, product)
            if str(product.metafields.for_who).strip(): 
                metafields.append({"namespace": "my_fields", "key": "for_who", "value": str(product.metafields.for_who).strip(), "type": "single_line_text_field"})
                metafields.append({"namespace": "italian", "key": "per_chi", "value": str(product.metafields.for_who).strip(), "type": "single_line_text_field"})
            if str(product.frame_color).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'frame_color', "value": str(product.frame_color).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'colore_della_montatura', "value": str(product.frame_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_material).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'frame_material', "value": str(product.metafields.frame_material).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'materiale_della_montatura', "value": str(product.metafields.frame_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_shape).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'frame_shape', "value": str(product.metafields.frame_shape).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'forma', "value": str(product.metafields.frame_shape).strip(), "type": "single_line_text_field"})
            if str(product.lens_color).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'lens_color', "value": str(product.lens_color).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'colore_della_lente', "value": str(product.lens_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_material).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'lens_material', "value": str(product.metafields.lens_material).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'materiale_della_lente', "value": str(product.metafields.lens_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_technology).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'lens_technology', "value": str(product.metafields.lens_technology).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'tecnologia_della_lente', "value": str(product.metafields.lens_technology).strip(), "type": "single_line_text_field"})
            if str(product.metafields.product_size).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'product_size', "value": str(product.metafields.product_size).strip(), "type": "single_line_text_field"})
                metafields.append({'namespace': 'italian', 'key': 'calibro_ponte_asta', "value": str(product.metafields.product_size).strip(), "type": "single_line_text_field"})
            if str(product.metafields.gtin1).strip(): 
                metafields.append({'namespace': 'my_fields', 'key': 'gtin1', "value": str(product.metafields.gtin1).strip(), "type": "single_line_text_field"})
            if str(meta_title).strip(): metafields.append({'namespace': 'global', 'key': 'title_tag', "value": str(meta_title).strip(), "type": "single_line_text_field"}) 
            if str(meta_description).strip(): metafields.append({'namespace': 'global', 'key': 'description_tag', "value": str(meta_description).strip(), "type": "single_line_text_field"})
            
        except Exception as e: 
            self.print_logs(f'Exception in get_new_product_metafeilds: {e}')
            if self.DEBUG: print(f'Exception in get_new_product_metafeilds: {e}')
            else: pass
        finally: return metafields
    
    # check product variant and update them if needed
    def check_product_variant(self, new_product_title: str, variant: Variant, product: Product, shopify_product: dict, shopify_processor: Shopify_Processor) -> None:
        try:
            matched_index = self.get_matched_variant(variant, shopify_product['variants'])
                                        
            if matched_index != -1:
                if str(variant.title).strip():
                    if len(product.variants) == 1:
                        if 'Default Title' != shopify_product['variants'][matched_index]['title']:
                            if not shopify_processor.update_variant_title(variant.shopify_id, 'Default Title', new_product_title):
                                self.print_logs(f'Failed to update variant title of product: {new_product_title}')
                    else:    
                        if variant.title != shopify_product['variants'][matched_index]['title']:
                            if not shopify_processor.update_variant_title(variant.shopify_id, str(variant.title).strip(), new_product_title):
                                self.print_logs(f'Failed to update variant title of product: {new_product_title}')

                if str(variant.sku).strip() and str(variant.sku).strip() != str(shopify_product['variants'][matched_index]['sku']).strip():
                    if not shopify_processor.update_variant_sku(variant.shopify_id, str(variant.sku).strip(), new_product_title):
                        self.print_logs(f'Failed to update variant sku of product: {new_product_title}')

                if str(variant.listing_price).strip() and str(variant.listing_price).strip() != str(shopify_product['variants'][matched_index]['compare_at_price']).strip():
                    if not shopify_processor.update_variant_compare_at_price(variant.shopify_id, str(variant.listing_price).strip(), new_product_title):
                        self.print_logs(f'Failed to update variant price of product: {new_product_title}')

                if int(variant.inventory_quantity) != int(shopify_product['variants'][matched_index]['inventory_quantity']):
                    adjusted_qunatity = shopify_processor.get_adjusted_inventory_level(int(variant.inventory_quantity), int(shopify_product['variants'][matched_index]['inventory_quantity']))
                    if not shopify_processor.update_variant_inventory_quantity(variant.inventory_item_id, adjusted_qunatity, int(variant.inventory_quantity), new_product_title):
                        self.print_logs(f'Failed to update variant inventory quantity of product: {new_product_title}')

                if str(variant.barcode_or_gtin).strip() and str(variant.barcode_or_gtin).strip() != str(shopify_product['variants'][matched_index]['barcode']).strip():
                    if not shopify_processor.update_variant_barcode(variant.shopify_id, str(variant.barcode_or_gtin).strip(), new_product_title):
                        self.print_logs(f'Failed to update variant barcode of product: {new_product_title}')
            else:
                self.print_logs(f'{variant.title} not matched with any')
        except Exception as e:
            if self.DEBUG: print(f'Exception in check_product_variant: {e}')
            else: pass
    
    # get matched database varinat from shopify variants
    def get_matched_variant(self, database_variant: Variant, shopify_variants: list[dict]) -> int:
        matched_index = -1
        try:
            for index, shopify_variant in enumerate(shopify_variants):
                if str(shopify_variant['id']).strip() == str(database_variant.shopify_id).strip():
                    matched_index = index
                    break
        except Exception as e: 
            self.print_logs(f'Exception in create_product_title: {e}')
            if self.DEBUG: print(f'Exception in create_product_title: {e}')
            else: pass
        finally: return matched_index

    # add new product variant to the shopify store
    def add_new_variant(self, variant: Variant, product: Product, new_product_title: str, shopify_processor: Shopify_Processor) -> None:
        try:
            self.print_logs('Adding variant', variant.sku)
            # inserting new variant to the shopify store
            shopify_processor.insert_variant(product, variant, new_product_title)
            # updating shopify_id and inventory_item_id of variant in database
            self.query_processor.update_variant_shopify_id(variant.shopify_id, variant.id)
            self.query_processor.update_variant_inventory_item_id(variant.inventory_item_id, variant.id)
            self.new_variants.append(variant)

            again_shopify_product = shopify_processor.get_product_from_shopify(product.shopify_id)
            for shopify_variant in again_shopify_product['product']['variants']:
                for v in product.variants:
                    if str(v.shopify_id).strip() == str(shopify_variant['id']).strip():
                        if str(v.title).strip() != str(shopify_variant['title']).strip():
                            if not shopify_processor.update_variant_title(v.shopify_id, str(v.title).strip(), new_product_title):
                                self.print_logs(f'Failed to update variant title of product: {new_product_title}')

            if len(product.variants) > 1 and again_shopify_product['product']['options']:
                for option in again_shopify_product['product']['options']:
                    if option['name'] == 'Title':
                        shopify_processor.update_product_options(option['product_id'], option['id'], 'Size')
        except Exception as e:
            self.print_logs(f'Exception in add_new_variant: {e}')
            if self.DEBUG: print(f'Exception in add_new_variant: {e}')
            else: pass

    # add new product to the shopify store
    def add_new_product(self, new_product_title: str, product: Product, brand: Brand, shopify_processor: Shopify_Processor) -> None:
        try:
            self.print_logs('New', new_product_title)
            # add this product as new to the store
            # get product description for new product
            product_description = self.create_product_description(brand, product)
            # get tags for new product
            tags = self.get_product_tags(brand, product, [])
            tags.insert(0, 'New')
            shopify_processor.insert_product(brand, product, new_product_title, product_description, tags)
            if product.shopify_id:
                # get metafields for new product
                new_metafields = self.get_new_product_metafeilds(brand, product)
                # adding metafields to new products
                for metafield in new_metafields: 
                    shopify_processor.set_metafields_for_product(product.shopify_id, metafield)
                image_description = self.create_product_image_description(brand, product)
                # adding 360 images to product
                self.add_product_360_images(product, image_description, shopify_processor)
                # updating shopify_id of new inserted product in database
                self.query_processor.update_product_shopify_id(product.shopify_id, product.id)
                # updating shopify_id and inventory_item_id of new variants in database
                for variant in product.variants:
                    self.query_processor.update_variant_shopify_id(variant.shopify_id, variant.id)
                    self.query_processor.update_variant_inventory_item_id(variant.inventory_item_id, variant.id)
                self.new_products.append(product)
        except Exception as e: 
            self.print_logs(f'Exception in add_new_product: {e}')
            if self.DEBUG: print(f'Exception in add_new_product: {e}')
            else: pass

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