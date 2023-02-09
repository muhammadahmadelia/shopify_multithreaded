import os
import json
import base64
import requests
from PIL import Image
from time import sleep
from urllib.parse import quote
from models.brand import Brand
from models.product import Product
from models.store import Store
from models.variant import Variant
from modules.files_reader import Files_Reader
from concurrent.futures import ThreadPoolExecutor
from functools import partial

class Shopify_Processor:
    def __init__(self, DEBUG: bool, config_file: str, logs_filename: str) -> None:
        self.DEBUG: bool = DEBUG
        self.config_file: str = config_file
        self.URL: str = ''
        self.session = requests.session()
        self.location_id: str = ''
        self.logs_filename = logs_filename
        pass

    # get shopify store id
    def get_store_url(self) -> None:
        try:
            # reading config file
            file_reader = Files_Reader(self.DEBUG)
            json_data = file_reader.read_json_file(self.config_file)

            API_KEY, PASSWORD, shop_name, Version = json_data[0]['API Key'], json_data[0]['Admin API Access token'], json_data[0]['store name'], json_data[0]['Version']
            self.URL = f'https://{API_KEY}:{PASSWORD}@{shop_name}.myshopify.com/admin/api/{Version}/'
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store_url: {str(e)}')
            self.print_logs(f'Exception in get_store_url: {str(e)}')

     # shopify stuff
    ### product ###
    # get all products from shopify against vendor name
    def get_products_by_vendor(self, vendor_name: str) -> list[dict]:
        products = []
        try:
            endpoint = ''
            endpoint = f'products.json?limit=250&vendor={quote(vendor_name)}'
            url =  self.URL + endpoint
            while True:
                response = ''
                while True:
                    response = requests.get(url=url, stream=True)
                    if response.status_code == 200:
                        json_data = json.loads(response.text)
                        products += list(json_data['products'])
                        break
                    elif response.status_code == 429: sleep(0.1)
                    else: 
                        self.print_logs(f'{response.status_code} found in getting products by vendor')
                        sleep(1)
                try:
                    page_info = ''
                    link = str(response.headers['Link']).strip()
                    if 'rel="next"' in link:
                        if len(link.split(';')) == 2: page_info = str(link).split(';')[0][1:-1]
                        elif len(link.split(';')) == 3:
                            page_info = str(link).split(';')[-2]
                            page_info = page_info.split(',')[-1].strip()[1:-1]
                        url = page_info.split('page_info=')[-1]
                        url = self.URL + f'products.json?limit=250&page_info=' + page_info
                    else: break
                except: break

        except Exception as e:
            if self.DEBUG: print(f'Exception in get_products_by_vendor: {str(e)}')
            self.print_logs(f'Exception in get_products_by_vendor: {str(e)}')
        finally: return products

    # get product from shopify against product id
    def get_product_from_shopify(self, product_id: str) -> dict:
        shopify_product = {}
        try:
            flag = False
            endpoint = f'products/{product_id}.json'
            while not flag:
                response = self.session.get(url=(self.URL + endpoint))
                if response.status_code == 200: 
                    shopify_product = json.loads(response.text)
                    flag = True
                elif response.status_code == 429: sleep(0.1)
                elif response.status_code == 404: break
                else: 
                    self.print_logs(f'{response.status_code} found in getting product', response.text)
                    sleep(1)
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product: {e}')
            self.print_logs(f'Exception in get_product: {e}')
        finally: return shopify_product

    # update product title against product id
    def update_product_title(self, product_id: str, title: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "title": str(title).strip() } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product title')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_title: {e}')
            self.print_logs(f'Exception in update_product_title: {e}')
        finally: return update_flag

    # update product body html against product id
    def update_product_body_html(self, product_id: str, body_html: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "body_html": str(body_html).strip() } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product body_html')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_body_html: {e}')
            self.print_logs(f'Exception in update_product_body_html: {e}')
        finally: return update_flag

    # update product status against product id
    def update_product_status(self, product_id: str, status: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "status": str(status).strip().lower() } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product status')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_status: {e}')
            self.print_logs(f'Exception in update_product_status: {e}')
        finally: return update_flag
    
    # update product type against product id
    def update_product_type(self, product_id: str, type: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "product_type": str(type).strip() } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product type')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_type: {e}')
            self.print_logs(f'Exception in update_product_type: {e}')
        finally: return update_flag

    # update product tags against product id
    def update_product_tags(self, product_id: str, tags: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "tags": str(tags).strip() } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product tags')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_tags: {e}')
            self.print_logs(f'Exception in update_product_tags: {e}')
        finally: return update_flag

    # update product image against product id
    def update_product_image(self, product_id: str, image_attachment, filename: str, alt: str, product_title: str) -> bool:
        updation_flag = False
        try:
            # filename = f"{str(product_title).split('-')[0].strip().lower().replace(' ', '-')}-01.jpg"
            # alt = f"{str(product_title).split('-')[0].strip()}"
            # image_attachment = self.download_image(img_url)
            # if image_attachment:
            #     image_attachment = base64.b64encode(image_attachment)
            endpoint = f'products/{product_id}/images.json'
            json_value = {"image": {"attachment": image_attachment.decode('utf-8'), "filename": filename, "alt": alt}}
            counter = 0
            while not updation_flag:
                response = self.session.post(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to add image to product: {product_title} {response.text}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        # else: self.print_logs(f'Failed to download image for {product_title}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_image: {str(e)}')
            self.print_logs(f'Exception in update_product_image: {str(e)}')
        return updation_flag

    # update product image against image id
    def update_product_image_alt_text(self, product_id: str, image_id: str, alt: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'products/{product_id}/images/{image_id}.json'
            json_value = {"image": {"id": image_id, "alt": alt}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update alt text of image: {product_title} status code: {response.status_code}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        # else: self.print_logs(f'Failed to download image for {product_title}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_image_alt_text: {str(e)}')
            self.print_logs(f'Exception in update_product_image_alt_text: {str(e)}')
        return updation_flag

    # this function will download image from the given url
    def download_image(self, url: str):
        image_attachment = ''
        try:
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-Encoding': 'gzip, deflate, br',
                'accept-Language': 'en-US,en;q=0.9',
                'cache-Control': 'max-age=0',
                'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'Sec-Fetch-User': '?1',
                'upgrade-insecure-requests': '1',
            }
            
            for _ in range(0, 10):
                try:
                    response = requests.get(url=url, headers=headers, stream=True)
                    if response.status_code == 200:
                        # image_attachment = base64.b64encode(response.content)
                        image_attachment = response.content
                        break
                    elif response.status_code == 404: 
                        self.print_logs(f'404 in downloading this image {url}')
                        break
                    else: 
                        self.print_logs(f'{response.status_code} found for downloading image')
                        sleep(1)
                except: pass
        except Exception as e:
            if self.DEBUG: print(f'Exception in download_image: {str(e)}')
            self.print_logs(f'Exception in download_image: {str(e)}')
        finally: return image_attachment

    def crop_downloaded_image(self, filename: str) -> None:
        try:
            im = Image.open(filename)
            width, height = im.size   # Get dimensions
            new_width = 1680
            new_height = 1020
            left = (width - new_width)/2
            top = (height - new_height)/2
            right = (width + new_width)/2
            bottom = (height + new_height)/2
            im = im.crop((left, top, right, bottom))
            try:
                im.save(filename)
            except:
                rgb_im = im.convert('RGB')
                rgb_im.save(filename)
        except Exception as e:
            if self.DEBUG: print(f'Exception in crop_downloaded_image: {e}')
            self.print_logs(f'Exception in crop_downloaded_image: {e}')

    # delete product image against product id and image id
    def delete_product_image(self, product_id: str, image_id: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'products/{product_id}/images/{image_id}.json'
            counter = 0
            while not updation_flag:
                response = self.session.delete(url=(self.URL + endpoint))
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to delete image to product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in delete_product_image: {str(e)}')
            self.print_logs(f'Exception in delete_product_image: {str(e)}')
        return updation_flag

    # update product option name against option id
    def update_product_options(self, product_id: str, option_id: str, option_name: str) -> bool:
        update_flag = False
        try:
            endpoint = f'products/{product_id}.json'
            json = { "product": { "id": product_id, "options": {"id": option_id, "name": option_name} } }
            counter = 0
            while not update_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json)
                if response.status_code == 200: update_flag = True                
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product option')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_options: {e}')
            self.print_logs(f'Exception in update_product_options: {e}')
        finally: return update_flag

    # insert new product
    def insert_product(self, brand: Brand, product: Product, product_title: str, product_description: str, title_tag: str, description_tag: str, image_description: str, store: Store):
        try:
            if not self.location_id: self.location_id = self.get_inventory_level_no_id()
            product_variants = []
            for index, variant in enumerate(product.variants):
                title = ''
                if len(product.variants) == 1: title = 'Default Title'
                else: title = variant.title
                if str(variant.listing_price).strip() == '': variant.listing_price = '0.00'
                json_single_variant = {
                    "option1": str(title), 
                    "price": str(variant.listing_price), 
                    "sku": str(variant.sku), 
                    "position": (index + 1),
                    "compare_at_price": str(variant.listing_price),
                    "taxable": 'true',
                    "barcode": str(variant.barcode_or_gtin),
                    "grams": 500, 
                    "weight": str(variant.weight), 
                    "weight_unit": 'kg',
                    "inventory_management": "shopify"
                }
                product_variants.append(json_single_variant)
            
            tags = ['New']
            if str(brand.name).strip(): tags.append(str(brand.name).strip())
            if str(product.number).strip(): tags.append(str(product.number).strip().upper())
            if str(product.name).strip() != '-': tags.append(str(product.name).strip().upper())
            if str(product.frame_code).strip(): tags.append(str(product.frame_code).strip().upper())
            if str(product.type).strip(): tags.append(str(product.type).strip())
            if str(product.metafields.for_who).strip():
                if str(product.metafields.for_who).strip().lower() == 'unisex':
                    tags.append('Men')
                    tags.append('Women')
                else: tags.append(str(product.metafields.for_who).strip())
            if str(product.metafields.activity).strip(): tags.append(str(product.metafields.activity).strip())
            if str(product.metafields.lens_material).strip(): tags.append(str(product.metafields.lens_material).strip())
            if str(product.metafields.graduabile).strip(): tags.append(str(product.metafields.graduabile).strip())
            if str(product.metafields.interest).strip(): tags.append(str(product.metafields.interest).strip())
            if str(product.metafields.lens_technology).strip(): tags.append(str(product.metafields.lens_technology).strip())
            if str(product.metafields.frame_shape).strip(): tags.append(str(product.metafields.frame_shape).strip())
            if str(product.metafields.frame_material).strip(): tags.append(str(product.metafields.frame_material).strip())
            
            product_json = {"product": {
                    "title": str(product_title),
                    "body_html": str(product_description).strip(),
                    "vendor": str(brand.name),
                    "product_type": str(product.type),
                    "template_suffix": "", 
                    "status": str(product.status), 
                    "published_scope": "web", 
                    "tags": tags,
                    "variants": product_variants
                }
            }

            metafields = []
            # title_tag, description_tag = '', ''
            # if str(product.name).strip(): 
            #     title_tag = f'{str(brand.name).strip().title()} {str(product.name).strip().upper()} {str(product.type).strip().title()} {str(product.number).strip().upper()} {str(product.frame_code).strip().upper()} - {str(product.frame_color).strip().title()} - {str(product.lens_color).strip().title()} | LookerOnline'
            # else: title_tag = f'{str(brand.name).strip().title()} {str(product.metafields.for_who).strip().title()} {str(product.type).strip().title()} {str(product.number).strip().upper()} {str(product.frame_code).strip().upper()} - {str(product.frame_color).strip().title()} - {str(product.lens_color).strip().title()} | LookerOnline'
            
            # description_tag = f'Buy {str(brand.name).strip().title()} {str(product.number).strip().upper()} {str(product.frame_code).strip().upper()} {str(product.name).strip().upper()} {str(product.metafields.for_who).strip().title()} {str(product.type).strip().title()}! ✓ Express WorldWide Shipping ✓ Secure Checkout ✓ 100% Original | LookerOnline |'
            # if '  ' in description_tag: description_tag = str(description_tag).strip().replace('  ', ' ')
            
            if str(product.metafields.for_who).strip(): metafields.append({"namespace": "my_fields", "key": "for_who", "value": str(product.metafields.for_who).strip(), "type": "single_line_text_field"})
            if str(product.frame_color).strip(): metafields.append({'namespace': 'my_fields', 'key': 'frame_color', "value": str(product.frame_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_material).strip(): metafields.append({'namespace': 'my_fields', 'key': 'frame_material', "value": str(product.metafields.frame_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_shape).strip(): metafields.append({'namespace': 'my_fields', 'key': 'frame_shape', "value": str(product.metafields.frame_shape).strip(), "type": "single_line_text_field"})
            if str(product.lens_color).strip(): metafields.append({'namespace': 'my_fields', 'key': 'lens_color', "value": str(product.lens_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_material).strip(): metafields.append({'namespace': 'my_fields', 'key': 'lens_material', "value": str(product.metafields.lens_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_technology).strip(): metafields.append({'namespace': 'my_fields', 'key': 'lens_technology', "value": str(product.metafields.lens_technology).strip(), "type": "single_line_text_field"})
            if str(product.metafields.product_size).strip(): metafields.append({'namespace': 'my_fields', 'key': 'product_size', "value": str(product.metafields.product_size).strip(), "type": "single_line_text_field"})
            if str(product.metafields.gtin1).strip(): metafields.append({'namespace': 'my_fields', 'key': 'gtin1', "value": str(product.metafields.gtin1).strip(), "type": "single_line_text_field"}) 
            if str(title_tag).strip(): metafields.append({'namespace': 'global', 'key': 'title_tag', "value": str(title_tag).strip(), "type": "single_line_text_field"}) 
            if str(description_tag).strip(): metafields.append({'namespace': 'global', 'key': 'description_tag', "value": str(description_tag).strip(), "type": "single_line_text_field"})

            if str(product.metafields.for_who).strip(): metafields.append({"namespace": "italian", "key": "per_chi", "value": str(product.metafields.for_who).strip(), "type": "single_line_text_field"})
            if str(product.frame_color).strip(): metafields.append({'namespace': 'italian', 'key': 'colore_della_montatura', "value": str(product.frame_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_material).strip(): metafields.append({'namespace': 'italian', 'key': 'materiale_della_montatura', "value": str(product.metafields.frame_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.frame_shape).strip(): metafields.append({'namespace': 'italian', 'key': 'forma', "value": str(product.metafields.frame_shape).strip(), "type": "single_line_text_field"})
            if str(product.lens_color).strip(): metafields.append({'namespace': 'italian', 'key': 'colore_della_lente', "value": str(product.lens_color).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_material).strip(): metafields.append({'namespace': 'italian', 'key': 'materiale_della_lente', "value": str(product.metafields.lens_material).strip(), "type": "single_line_text_field"})
            if str(product.metafields.lens_technology).strip(): metafields.append({'namespace': 'italian', 'key': 'tecnologia_della_lente', "value": str(product.metafields.lens_technology).strip(), "type": "single_line_text_field"})
            if str(product.metafields.product_size).strip(): metafields.append({'namespace': 'italian', 'key': 'calibro_ponte_asta', "value": str(product.metafields.product_size).strip(), "type": "single_line_text_field"})
            # if str(product.metafields.activity).strip(): metafields.append({'namespace': 'italian', 'key': 'attivita', "value": str(product.metafields.activity).strip(), "type": "single_line_text_field"})


            endpoint = 'products.json'
            flag = False
            while not flag:
                response = self.session.post(url=(self.URL + endpoint), json=product_json)
                if response.status_code == 201:
                    json_data = json.loads(response.text)
                    product.shopify_id = str(json_data['product']['id']).strip()
                    
                    for metafield in metafields: 
                        self.set_metafields_for_product(product.shopify_id, metafield)

                    for variant_counter, variant in enumerate(json_data['product']['variants']):
                        product.variants[variant_counter].shopify_id = variant['id']
                        product.variants[variant_counter].inventory_item_id = variant['inventory_item_id']
                        self.update_variant_inventory_quantity(product.variants[variant_counter].inventory_item_id, product.variants[variant_counter].inventory_quantity, product.variants[variant_counter].inventory_quantity, title)
                        self.set_country_code(product.variants[variant_counter].inventory_item_id)
                    
                    if product.metafields.img_360_urls:
                        # for digitalhub
                        if store.id == 1:
                            spin_images = 0
                            # for image_360_url in product.metafields.img_360_urls:
                            #     filename = image_360_url.split('/')[-1].strip()

                            #     image_attachment = self.download_image(image_360_url)
                            #     if image_attachment:
                            #         spin_images += 1
                            #         with open(filename, 'wb') as f: f.write(image_attachment)

                            #         self.crop_downloaded_image(filename)
                                    
                            #         f = open(filename, 'rb')
                            #         image_attachment = base64.b64encode(f.read())
                            #         f.close()
                                    
                            #         self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                            #         os.remove(filename)
                            images_url_and_name = []
                            for index2, image_360_url in enumerate(product.metafields.img_360_urls):
                                # filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                                filename = f'{product_title.replace(" ", "_").replace("/", "_")}_{index2 + 1}.jpg'
                                images_url_and_name.append({'filename': filename, 'img_url': image_360_url})

                            # with ThreadPoolExecutor(max_workers=len(images_url_and_name)) as e:
                            #     e.map(partial (self.save_downloaded_images), images_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                self.save_downloaded_images(image_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                filename = image_url_and_name['filename']
                                if os.path.exists(filename):
                                    spin_images += 1

                                    f = open(filename, 'rb')
                                    image_attachment = base64.b64encode(f.read())
                                    f.close()

                                    self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                                    os.remove(filename)

                            new_tags = f"{json_data['product']['tags']}, spinimages={spin_images}"
                            self.update_product_tags(product.shopify_id, new_tags)
                        # for safilo
                        elif store.id == 2:
                            spin_images = 0
                            # for image_360_url in product.metafields.img_360_urls:
                            #     filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                            #     image_attachment = self.download_image(image_360_url)
                            #     if image_attachment:
                            #         spin_images += 1
                            #         with open(filename, 'wb') as f: f.write(image_attachment)

                            #         f = open(filename, 'rb')
                            #         image_attachment = base64.b64encode(f.read())
                            #         f.close()
                                    
                            #         self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                            #         os.remove(filename)
                            images_url_and_name = []
                            for index2, image_360_url in enumerate(product.metafields.img_360_urls):
                                if image_360_url:
                                    # filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                                    filename = f'{product_title.replace(" ", "_").replace("/", "_")}_{index2 + 1}.jpg'
                                    images_url_and_name.append({'filename': filename, 'img_url': image_360_url})

                            # with ThreadPoolExecutor(max_workers=len(images_url_and_name)) as e:
                            #     e.map(partial (self.save_downloaded_images), images_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                self.save_downloaded_images(image_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                filename = image_url_and_name['filename']
                                if os.path.exists(filename):
                                    spin_images += 1

                                    f = open(filename, 'rb')
                                    image_attachment = base64.b64encode(f.read())
                                    f.close()

                                    self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                                    os.remove(filename)
                        # for rudy project
                        elif store.id == 4:
                            spin_images = 0
                            # for image_360_url in product.metafields.img_360_urls:
                            #     filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                            #     image_attachment = self.download_image(image_360_url)
                            #     if image_attachment:
                            #         spin_images += 1
                            #         with open(filename, 'wb') as f: f.write(image_attachment)

                            #         f = open(filename, 'rb')
                            #         image_attachment = base64.b64encode(f.read())
                            #         f.close()
                                    
                            #         self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                            #         os.remove(filename)
                            images_url_and_name = []
                            for index2, image_360_url in enumerate(product.metafields.img_360_urls):
                                # filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                                filename = f'{product_title.replace(" ", "_").replace("/", "_")}_{index2 + 1}.jpg'
                                images_url_and_name.append({'filename': filename, 'img_url': image_360_url})

                            # with ThreadPoolExecutor(max_workers=len(images_url_and_name)) as e:
                            #     e.map(partial (self.save_downloaded_images), images_url_and_name)
                            for image_url_and_name in images_url_and_name:
                                self.save_downloaded_images(image_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                filename = image_url_and_name['filename']
                                if os.path.exists(filename):
                                    spin_images += 1

                                    f = open(filename, 'rb')
                                    image_attachment = base64.b64encode(f.read())
                                    f.close()

                                    self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                                    os.remove(filename)
                            if len(product.metafields.img_360_urls) > 1:     
                                new_tags = f"{json_data['product']['tags']}, spinimages={spin_images}"
                                self.update_product_tags(product.shopify_id, new_tags)
                        # for luxottica
                        elif store.id == 5:
                            spin_images = 0
                            # for image_360_url in product.metafields.img_360_urls:
                            #     filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                            #     if '?' in filename: filename = str(filename).split('?')[0].strip()
                            #     # if '.png' in filename: filename = str(filename).replace('.png', '.jpg').strip()
                            #     # print(filename)
                                
                            #     image_attachment = self.download_image(image_360_url)
                            #     if image_attachment:
                            #         spin_images += 1
                            #         with open(filename, 'wb') as f: f.write(image_attachment)

                            #         f = open(filename, 'rb')
                            #         image_attachment = base64.b64encode(f.read())
                            #         f.close()
                                    
                            #         self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                            #         os.remove(filename)
                            images_url_and_name = []
                            for index2, image_360_url in enumerate(product.metafields.img_360_urls):
                                # filename = str(image_360_url.split('/')[-1].strip().split('?')[0])[1:].strip()
                                # if '?' in filename: filename = str(filename).split('?')[0].strip()
                                filename = f'{product_title.replace(" ", "_").replace("/", "_")}_{index2 + 1}.png'
                                if '?impolicy=MYL_EYE&wid=688' in image_360_url: image_360_url = image_360_url.replace('?impolicy=MYL_EYE&wid=688', '')
                                images_url_and_name.append({'filename': filename, 'img_url': image_360_url})

                            # with ThreadPoolExecutor(max_workers=len(images_url_and_name)) as e:
                            #     e.map(partial (self.save_downloaded_images), images_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                self.save_downloaded_images(image_url_and_name)

                            for image_url_and_name in images_url_and_name:
                                filename = image_url_and_name['filename']
                                if os.path.exists(filename):
                                    spin_images += 1

                                    f = open(filename, 'rb')
                                    image_attachment = base64.b64encode(f.read())
                                    f.close()

                                    self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)
                                    os.remove(filename)

                            new_tags = f"{json_data['product']['tags']}, spinimages={spin_images}"
                            self.update_product_tags(product.shopify_id, new_tags)
                    elif product.metafields.img_url:
                        image_attachment = self.download_image(str(product.metafields.img_url).strip())
                        if image_attachment:
                            image_attachment = base64.b64encode(image_attachment)
                            filename = f"{str(product_title).split('-')[0].strip().lower().replace(' ', '-')}-01.jpg"
                            # alt = f"{str(product_title).split('-')[0].strip()}"
                            self.update_product_image(product.shopify_id, image_attachment, filename, image_description, product_title)

                    if len(product.variants) > 1 and json_data['product']['options']:
                        for option in json_data['product']['options']:
                            if option['name'] == 'Title':
                                self.update_product_options(option['product_id'], option['id'], 'Size')

                    flag = True
                elif response.status_code == 429 or response.status_code == 430: sleep(1)
                else: 
                    self.print_logs(f'{response.status_code} found by inserting product to shopify: {product_title} Text: {response.text}')
                    break

            
        except Exception as e:
            if self.DEBUG: print(f'Exception in insert_product: {str(e)}')
            self.print_logs(f'Exception in insert_product: {str(e)}')
    
    def save_downloaded_images(self, image_url_and_name: dict) -> None:
        try:
            filename = image_url_and_name['filename']
            image_url = image_url_and_name['img_url']
            image_attachment = self.download_image(image_url)
            if image_attachment: 
                with open(filename, 'wb') as f: f.write(image_attachment)
            else: self.print_logs(f'Failed to download image from url: {image_url}')

        except Exception as e:
            if self.DEBUG: print(f'Exception in save_downloaded_images: {str(e)}')
            self.print_logs(f'Exception in save_downloaded_images: {str(e)}')
    
    ## metafields ##
    # get product metafileds
    def get_product_metafields_from_shopify(self, product_id: str) -> dict:
        shopify_product_metafields = {}
        try:
            flag = False
            endpoint = f'products/{product_id}/metafields.json'
            while not flag:
                response = self.session.get(url=(self.URL + endpoint))
                if response.status_code == 200: 
                    shopify_product_metafields = json.loads(response.text)
                    flag = True
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in getting product metafields', response.text)
                    sleep(1)
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_metafields: {e}')
            self.print_logs(f'Exception in get_product_metafields: {e}')
        finally: return shopify_product_metafields


    # get product italian metafileds
    def get_product_italian_metafields_from_shopify(self, product_id: str) -> dict:
        shopify_product_italian_metafields = {}
        try:
            flag = False
            endpoint = f'products/{product_id}/metafields.json'
            while not flag:
                response = self.session.get(url=(self.URL + endpoint), params={'namespace': 'italian'})
                if response.status_code == 200: 
                    shopify_product_italian_metafields = json.loads(response.text)
                    flag = True
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in getting product italian metafields', response.text)
                    sleep(1)
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_italian_metafields_from_shopify: {e}')
            self.print_logs(f'Exception in get_product_italian_metafields_from_shopify: {e}')
        finally: return shopify_product_italian_metafields

    # update for_who metafield against metafield id
    def update_for_who_metafield(self, metafield_id: str, for_who: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": for_who, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update for_who metafield for product: {product_title}', response.text, response.status_code)
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_for_who_metafield: {str(e)}')
            self.print_logs(f'Exception in update_for_who_metafield: {str(e)}')
        return updation_flag

    # update frame_color metafield against metafield id
    def update_frame_color_metafield(self, metafield_id: str, frame_color: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": frame_color, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update frame_color metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_frame_color_metafield: {str(e)}')
            self.print_logs(f'Exception in update_frame_color_metafield: {str(e)}')
        return updation_flag

    # update frame_material metafield against metafield id
    def update_frame_material_metafield(self, metafield_id: str, frame_material: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": frame_material, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update frame_material metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_frame_material_metafield: {str(e)}')
            self.print_logs(f'Exception in update_frame_material_metafield: {str(e)}')
        return updation_flag

    # update frame_shape metafield against metafield id
    def update_frame_shape_metafield(self, metafield_id: str, frame_shape: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": frame_shape, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update frame_shape metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_frame_shape_metafield: {str(e)}')
            self.print_logs(f'Exception in update_frame_shape_metafield: {str(e)}')
        return updation_flag

    # update lens_color metafield against metafield id
    def update_lens_color_metafield(self, metafield_id: str, lens_color: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": lens_color, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update lens_color metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_lens_color_metafield: {str(e)}')
            self.print_logs(f'Exception in update_lens_color_metafield: {str(e)}')
        return updation_flag

    # update lens_technology metafield against metafield id
    def update_lens_technology_metafield(self, metafield_id: str, lens_technology: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": lens_technology, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update lens_technology metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_lens_technology_metafield: {str(e)}')
            self.print_logs(f'Exception in update_lens_technology_metafield: {str(e)}')
        return updation_flag

    # update lens_material metafield against metafield id
    def update_lens_material_metafield(self, metafield_id: str, lens_material: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": lens_material, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update lens_material metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_lens_material_metafield: {str(e)}')
            self.print_logs(f'Exception in update_lens_material_metafield: {str(e)}')
        return updation_flag

    # update product_size metafield against metafield id
    def update_product_size_metafield(self, metafield_id: str, product_size: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": product_size, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update product_size metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_size_metafield: {str(e)}')
            self.print_logs(f'Exception in update_product_size_metafield: {str(e)}')
        return updation_flag

    # update gtin1 metafield against metafield id
    def update_gtin1_metafield(self, metafield_id: str, gtin1: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": gtin1, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update gtin1 metafield for product: {product_title}',json_value)
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_gtin1_metafield: {str(e)}')
            self.print_logs(f'Exception in update_gtin1_metafield: {str(e)}')
        return updation_flag

    # update activity metafield against metafield id
    def update_activity_metafield(self, metafield_id: str, activity: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": activity, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update activity metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in activity: {str(e)}')
            self.print_logs(f'Exception in activity: {str(e)}')
        return updation_flag

    # update for_who metafield against metafield id
    def update_graduabile_metafield(self, metafield_id: str, graduabile: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": graduabile, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update graduabile metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_graduabile_metafield: {str(e)}')
            self.print_logs(f'Exception in update_graduabile_metafield: {str(e)}')
        return updation_flag

    # update interest metafield against metafield id
    def update_interest_metafield(self, metafield_id: str, interest: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": interest, "type": "single_line_text_field"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update interest metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_interest_metafield: {str(e)}')
            self.print_logs(f'Exception in update_interest_metafield: {str(e)}')
        return updation_flag

    # update description_tag metafield against metafield id
    def update_description_tag_metafield(self, metafield_id: str, description_tag: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": description_tag, "type": "string"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value, timeout=20)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update description_tag metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_description_tag_metafield: {str(e)}')
            self.print_logs(f'Exception in update_description_tag_metafield: {str(e)}')
        return updation_flag

    # update title_tag metafield against metafield id
    def update_title_tag_metafield(self, metafield_id: str, title_tag: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'metafields/{metafield_id}.json'
            json_value = {"metafield": {"id": metafield_id, "value": title_tag, "type": "string"}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update title_tag metafield for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_title_tag_metafield: {str(e)}')
            self.print_logs(f'Exception in update_title_tag_metafield: {str(e)}')
        return updation_flag

    def set_metafields_for_product(self, product_id: str, metafield: dict) -> bool:
        metafield_flag = False
        try:
            endpoint = f'products/{product_id}/metafields.json'
            json_data = {"metafield":  metafield}
            counter = 0
            while not metafield_flag:
                response = self.session.post(url=(self.URL + endpoint), json=json_data)
                if response.status_code == 201: metafield_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'{response.status_code} in adding product metafield {json_data} {response.text}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in set_metafields_for_product: {str(e)}')
            self.print_logs(f'Exception in set_metafields_for_product: {str(e)}')
        finally: return metafield_flag


    ## variant ##
    # update variant title against variant id
    def update_variant_title(self, variant_id: str, title: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'variants/{variant_id}.json'
            json_value = {"variant": {"id": variant_id, "option1": title}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update title {title} of variant for product: {product_title} {response.text}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_title: {str(e)}')
            self.print_logs(f'Exception in update_variant_title: {str(e)}')
        return updation_flag

    # update variant sku against variant id
    def update_variant_sku(self, variant_id: str, sku: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'variants/{variant_id}.json'
            json_value = {"variant": {"id": variant_id, "sku": sku}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update sku of variant for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_sku: {str(e)}')
            self.print_logs(f'Exception in update_variant_sku: {str(e)}')
        return updation_flag

    # update variant price against variant id
    def update_variant_price(self, variant_id: str, price: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'variants/{variant_id}.json'
            json_value = {"variant": {"id": variant_id, "price": price}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update price of variant for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_price: {str(e)}')
            self.print_logs(f'Exception in update_variant_price: {str(e)}')
        return updation_flag

    # update variant compare_at_price against variant id
    def update_variant_compare_at_price(self, variant_id: str, price: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'variants/{variant_id}.json'
            json_value = {"variant": {"id": variant_id, "compare_at_price": price}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update compare_at_price of variant for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_compare_at_price: {str(e)}')
            self.print_logs(f'Exception in update_variant_compare_at_price: {str(e)}')
        return updation_flag

    # get the location id of the inventory
    def get_inventory_level(self, inventory_item_id: str) -> str:
        location_id = ''
        try:
            endpoint = f'inventory_levels.json?inventory_item_ids={inventory_item_id}'
            counter = 0
            while not location_id:
                response = self.session.get(url=(self.URL + endpoint))
                if response.status_code == 200: 
                    json_data = json.loads(response.text)
                    inventory_levels = json_data['inventory_levels']
                    location_id = inventory_levels[0]['location_id']
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} getting inventory level')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_inventory_level: {str(e)}')
            self.print_logs(f'Exception in get_inventory_level: {str(e)}')
        finally: return location_id

    # get the location id without inventory item id
    def get_inventory_level_no_id(self):
        location_id = ''
        try:
            endpoint = f'locations.json'
            counter = 0
            while not location_id:
                response = self.session.get(url=(self.URL + endpoint))
                if response.status_code == 200: 
                    json_data = json.loads(response.text)
                    inventory_levels = json_data['locations']
                    location_id = inventory_levels[0]['id']
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} getting inventory level')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_inventory_level: {str(e)}')
            self.print_logs(f'Exception in get_inventory_level: {str(e)}')
        finally: return location_id

    # get new adjusted quantity with respect to new qunatity
    def get_adjusted_inventory_level(self, new_quantity: int, old_quantity: int):
        adjusted_qunatity = 0
        try:
            while (int(old_quantity) + int(adjusted_qunatity)) != int(new_quantity):
                if int(old_quantity) > int(new_quantity): adjusted_qunatity -= 1
                elif int(old_quantity) < int(new_quantity): adjusted_qunatity += 1

            # if int(old_quantity) == 0 and int(new_quantity) == 1: adjusted_qunatity = 1
            # elif int(old_quantity) == 1 and int(new_quantity) == 0: adjusted_qunatity = -1
            # elif int(old_quantity) == 0 and int(new_quantity) == 0: adjusted_qunatity = 0
            # elif int(old_quantity) == 1 and int(new_quantity) == 1: adjusted_qunatity = 1
            # if int(new_quantity) < int(old_quantity):
            #     while True:
            #         if (adjusted_qunatity - old_quantity) == new_quantity: break
            #         else: adjusted_qunatity -= 1
            # elif int(new_quantity) > int(old_quantity):
            #     while True:
            #         if (adjusted_qunatity + old_quantity) == new_quantity: break
            #         else: adjusted_qunatity += 1
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_adjusted_inventory_level: {str(e)}')
            self.print_logs(f'Exception in get_adjusted_inventory_level: {str(e)}')
        finally: return adjusted_qunatity

    # update variant inventory_quantity against variant id
    def update_variant_inventory_quantity(self, inventory_item_id: str, inventory_quantity: int, new_inventory_quantity: int, product_title: str) -> bool:
        updation_flag = False
        try:
            if not self.location_id: self.location_id = self.get_inventory_level_no_id()
            if not self.location_id: self.location_id = self.get_inventory_level(inventory_item_id)
            endpoint = f'inventory_levels/adjust.json'
            json_data = {"location_id": self.location_id, "inventory_item_id": int(inventory_item_id), "available_adjustment": inventory_quantity}
            counter = 0
            while not updation_flag:
                response = self.session.post(url=(self.URL + endpoint), json=json_data)
                if response.status_code == 200:
                    # if json.loads(response.text)['inventory_level']['available'] == new_inventory_quantity: 
                    updation_flag = True
                    # else: self.print_logs(response.text)
                elif response.status_code == 429: sleep(0.1)
                else: 
                    self.print_logs(f'{response.status_code} found in updating product inventory quantity of product: {product_title} {response.text}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_inventory_item: {str(e)}')
            self.print_logs(f'Exception in update_inventory_item: {str(e)}')
        finally: return updation_flag

    # update variant barcode against variant id
    def update_variant_barcode(self, variant_id: str, barcode: str, product_title: str) -> bool:
        updation_flag = False
        try:
            endpoint = f'variants/{variant_id}.json'
            json_value = {"variant": {"id": variant_id, "barcode": barcode}}
            counter = 0
            while not updation_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_value)
                if response.status_code == 200: updation_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'Failed to update barcode of variant for product: {product_title}')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_barcode: {str(e)}')
            self.print_logs(f'Exception in update_variant_barcode: {str(e)}')
        return updation_flag

    def set_country_code(self, inventory_item_id: str) -> bool:
        country_code_flag = False
        try:
            endpoint = f'inventory_items/{inventory_item_id}.json'
            json_data = {"inventory_item": {"country_code_of_origin": "IT"}}
            counter = 0
            while not country_code_flag:
                response = self.session.put(url=(self.URL + endpoint), json=json_data)
                if response.status_code == 200: country_code_flag = True
                elif response.status_code == 429: sleep(1)
                else: 
                    self.print_logs(f'{response.status_code} in adding country code')
                    sleep(1)
                counter += 1
                if counter == 10: break
        except Exception as e:
            if self.DEBUG: print(f'Exception in set_country_code: {str(e)}')
            self.print_logs(f'Exception in set_country_code: {str(e)}')
        finally: return country_code_flag

    # insert new variant
    def insert_variant(self, product: Product, variant: Variant, product_title: str):
        try:
            if not self.location_id: self.location_id = self.get_inventory_level_no_id()
            title = ''
            if len(product.variants) == 1: title = 'Default Title'
            else: title = variant.title
            if variant.listing_price == '': variant.listing_price = '0.00'
            json_single_variant = {
                'variant': {
                    "product_id": product.shopify_id,
                    "option1": str(title), 
                    "price": str(variant.listing_price), 
                    "sku": str(variant.sku), 
                    "position": int(variant.position),
                    "compare_at_price": str(variant.listing_price),
                    "taxable": 'true',
                    "grams": 500, 
                    "weight": str(variant.weight), 
                    "weight_unit": 'kg',
                    "inventory_management": "shopify"
                }
            }


            endpoint = f'products/{product.shopify_id}/variants.json'
            flag = False
            counter = 0
            while not flag:
                response = self.session.post(url=(self.URL + endpoint), json=json_single_variant)
                if response.status_code == 201:
                    json_data = json.loads(response.text)

                    variant.shopify_id = str(json_data['variant']['id']).strip()
                    variant.inventory_item_id = str(json_data['variant']['inventory_item_id']).strip()

                    self.update_variant_inventory_quantity(variant.inventory_item_id, variant.inventory_quantity, variant.inventory_quantity, product_title)
                    self.set_country_code(variant.inventory_item_id)

                    
                    # if len(product.variants) > 1:
                    #     for other_variant in product.variants:
                    #         if other_variant.title != variant.title and other_variant.sku != variant.sku:
                    #             if not self.update_variant_title(variant.shopify_id, other_variant.title, product_title):
                    #                 self.print_logs(f'Failed to update variant title of product: {product_title}')

                    flag = True
                elif response.status_code == 429: sleep(1)
                else:
                    sleep(1)
                    self.print_logs(f'{response.status_code} {response.text} found by inserting variant {title} to the product : {product_title}')
                    counter += 1
                    if counter == 10: break
            
        except Exception as e:
            if self.DEBUG: print(f'Exception in insert_variant: {str(e)}')
            self.print_logs(f'Exception in insert_variant: {str(e)}')

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass