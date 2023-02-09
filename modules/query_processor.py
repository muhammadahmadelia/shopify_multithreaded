from datetime import datetime
from models.store import Store
from models.brand import Brand
from models.product import Product
from models.metafields import Metafields
from models.variant import Variant
from modules.db_connection import DB_Connection

class Query_Processor:
    def __init__(self, DEBUG: bool, connection_string: str):
        self.DEBUG = DEBUG
        self.connection_string = connection_string
        self.db_connection_obj = None
        pass

    def get_db_obj(self):
        try:
            if not self.db_connection_obj:
                # setting connection values
                self.db_connection_obj = DB_Connection(self.DEBUG)
                self.db_connection_obj.setting_connection_values(self.connection_string)
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_connection_string: {e}')
            else: pass
    
    
    # def process_query_many_insertions(self, db, query, values):
    #     try:
    #         cursor = db.cursor()
    #         cursor.executemany(query, values)
    #         db.commit()
    #         return cursor
    #     except Exception as e:
    #         if self.DEBUG: print('Exception in process_query_insertions: '+ str(e))
    #         else: pass

    # def process_query_single_insertions(self, db, query, values):
    #     try:
    #         cursor = db.cursor()
    #         cursor.execute(query, values)
    #         db.commit()
    #         return cursor
    #     except Exception as e:
    #         if self.DEBUG: print('Exception in process_query_insertions: '+ str(e), query, values)
    #         else: pass

    # def process_query_updations(self, db, query):
    #     try:
    #         cursor = db.cursor()
    #         cursor.execute(query)
    #         db.commit()
    #         return cursor
    #     except Exception as e:
    #         if self.DEBUG: print('Exception in process_query_insertions: '+ str(e))
    #         else: pass
    
    # def process_query_selections(self, db, query):
    #     try:
    #         cursor = db.cursor()
    #         cursor.execute(query)
    #         return cursor
    #     except Exception as e:
    #         if self.DEBUG: print('Exception in process_query_selections: '+ str(e))
    #         else: pass


    # # store
    # get all stores from database
    def get_stores(self) -> list[Store]:
        stores = []
        try:
            query = f'SELECT * FROM stores ORDER BY id'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for store_data in cursor.fetchall():
                store = Store()
                store.id = store_data['id']
                store.name = store_data['name']
                store.link = store_data['link']
                store.login_flag = store_data['login_flag']
                store.username = store_data['username']
                store.password = store_data['password']
                stores.append(store)
            if cursor: cursor.close()
            if db: db.commit()
            
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_stores: {e}')
            else: pass
        finally: return stores

    # get store from database against id
    def get_store_by_id(self, id: int) -> Store:
        store = Store()
        try:
            query = f'SELECT * FROM stores WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            store_data = cursor.fetchone()
            if store_data:
                store.id = store_data['id']
                store.name = store_data['name']
                store.link = store_data['link']
                store.login_flag = store_data['login_flag']
                store.username = store_data['username']
                store.password = store_data['password']
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store_by_id: {e}')
            else: pass
        finally: return store

    # get store from database against name
    def get_store_by_name(self, name: str) -> Store:
        store = Store()
        try:
            query = f'SELECT * FROM stores WHERE name = "{name}"'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            store_data = cursor.fetchone()
            if store_data:
                store.id = store_data['id']
                store.name = store_data['name']
                store.link = store_data['link']
                store.login_flag = store_data['login_flag']
                store.username = store_data['username']
                store.password = store_data['password']
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store_by_name: {e}')
            else: pass
        finally: return store

    # get store from database against product id
    def get_store_against_product_id(self, product_id: int) -> Store:
        store = Store()
        try:
            query = f'SELECT stores.* FROM stores INNER JOIN brands ON stores.id = brands.store_id INNER JOIN products ON brands.id = products.brand_id WHERE products.id = {product_id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            store_data = cursor.fetchone()
            if store_data:
                store.id = store_data['id']
                store.name = store_data['name']
                store.link = store_data['link']
                store.login_flag = store_data['login_flag']
                store.username = store_data['username']
                store.password = store_data['password']
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store: {e}')
            else: pass
        finally: return store

    # brand
    # get all brands from database
    def get_brands(self) -> list[Brand]:
        brands = []
        try:
            query = f'SELECT * FROM brands ORDER BY id'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for brand_data in cursor.fetchall():
                brand = Brand()
                brand.id = brand_data['id']
                brand.store_id = brand_data['store_id']
                brand.name = brand_data['name']
                brand.code = brand_data['code']
                brands.append(brand)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands: {e}')
            else: pass
        finally: return brands
    
    # get all brands from database against store_id
    def get_brands_by_store_id(self, store_id: int) -> list[Brand]:
        brands = []
        try:
            query = f'SELECT * FROM brands WHERE store_id = {store_id} ORDER BY id'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for brand_data in cursor.fetchall():
                brand = Brand()
                brand.id = brand_data['id']
                brand.store_id = brand_data['store_id']
                brand.name = brand_data['name']
                brand.code = brand_data['code']
                brands.append(brand)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands_by_store_id: {e}')
            else: pass
        finally: return brands

    # get all brands from database against store_name
    def get_brands_by_store_name(self, store_name: str) -> list[Brand]:
        brands = []
        try:
            query = f'SELECT brands.* FROM brands INNER JOIN stores ON brands.store_id = stores.id WHERE stores.name = "{store_name}" ORDER BY brands.id'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for brand_data in cursor.fetchall():
                brand = Brand()
                brand.id = brand_data['id']
                brand.store_id = brand_data['store_id']
                brand.name = brand_data['name']
                brand.code = brand_data['code']
                brands.append(brand)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands_by_store_name: {e}')
            else: pass
        finally: return brands
    
    # get brand from database against id
    def get_brand_by_id(self, id: int) -> Brand:
        brand = Brand()
        try:
            query = f'SELECT * FROM brands WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            brand_data= cursor.fetchone()
            if brand_data:
                brand.id = brand_data['id']
                brand.store_id = brand_data['store_id']
                brand.name = brand_data['name']
                brand.code = brand_data['code']
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands: {e}')
            else: pass
        finally: return brand

    # products
    # get all products from database against brand_id
    def get_products_by_brand_id(self,brand_id: int, condition: str = None) -> list[Product]:
        products = []
        try:
            query = ''
            if condition: query = f'SELECT * FROM products WHERE brand_id = {brand_id} AND {condition} ORDER BY id'
            else: query = f'SELECT * FROM products WHERE brand_id = {brand_id} ORDER BY id'

            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for product_data in cursor.fetchall():
                product = Product()
                product.id = product_data['id']
                product.brand_id = product_data['brand_id']
                product.number = product_data['number']
                product.name = product_data['name']
                product.frame_code = product_data['frame_code']
                product.frame_color = product_data['frame_color']
                product.lens_code = product_data['lens_code']
                product.lens_color = product_data['lens_color']
                product.status = product_data['status']
                product.type = product_data['type']
                product.url = product_data['url']
                product.shopify_id = product_data['shopify_id']
                products.append(product)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_products: {e}')
            else: pass
        finally: return products

    # get product from database against id
    def get_product_by_id(self,id: int) -> Product:
        product = Product()
        try:
            query = f'SELECT * FROM products WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            product_data = cursor.fetchone()
            if product_data:
                product.id = product_data['id']
                product.brand_id = product_data['brand_id']
                product.number = product_data['number']
                product.name = product_data['name']
                product.frame_code = product_data['frame_code']
                product.frame_color = product_data['frame_color']
                product.lens_code = product_data['lens_code']
                product.lens_color = product_data['lens_color']
                product.status = product_data['status']
                product.type = product_data['type']
                product.url = product_data['url']
                product.shopify_id = product_data['shopify_id']
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_products: {e}')
            else: pass
        finally: return product

    # get product from database against brand_id and created time
    def get_products_by_brand_id_and_created_time(self,brand_id: int, created_at: str) -> list[Product]:
        products = []
        try:
            query = f'SELECT * FROM products WHERE brand_id = {brand_id} AND created_at NOT LIKE "{created_at}%";'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for product_data in cursor.fetchall():
                product = Product()
                product.id = product_data['id']
                product.brand_id = product_data['brand_id']
                product.number = product_data['number']
                product.name = product_data['name']
                product.frame_code = product_data['frame_code']
                product.frame_color = product_data['frame_color']
                product.lens_code = product_data['lens_code']
                product.lens_color = product_data['lens_color']
                product.status = product_data['status']
                product.type = product_data['type']
                product.url = product_data['url']
                product.shopify_id = product_data['shopify_id']
                products.append(product)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_products_by_brand_id_and_created_time: {e}')
            else: pass
        finally: return products

    # update number against product id
    def update_product_number(self,number: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET number = "{number}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_number: {e}')
            else: pass

    # update name against product id
    def update_product_name(self,name: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET name = "{name}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_name: {e}')
            else: pass

    # update frame_code against product id
    def update_product_frame_code(self,frame_code: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET frame_code = "{frame_code}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_frame_code: {e}')
            else: pass

    # update frame_color against product id
    def update_product_frame_color(self,frame_color: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET frame_color = "{frame_color}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_frame_color: {e}')
            else: pass
    
    # update lens_code against product id
    def update_product_lens_code(self,lens_code: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET lens_code = "{lens_code}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_lens_code: {e}')
            else: pass

    # update lens_color against product id
    def update_product_lens_color(self,lens_color: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET lens_color = "{lens_color}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_lens_color: {e}')
            else: pass

    # update status against product id
    def update_product_status(self,status: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET status = "{status}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_status: {e}')
            else: pass

    # update type against product id
    def update_product_type(self,type: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET type = "{type}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_type: {e}')
            else: pass
    
    # update url against product id
    def update_product_url(self,url: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET url = "{url}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_url: {e}')
            else: pass

    # update shopify_id against product id
    def update_product_shopify_id(self,shopify_id: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE products SET shopify_id = "{shopify_id}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_shopify_id: {e}')
            else: pass

    # insert new product #
    def insert_product(self,brand_id: int, scraped_product : Product) -> int:
        new_inserted_product_id = 0
        try:
            number = str(scraped_product.number).strip().upper() if scraped_product.number else ''
            name = str(str(scraped_product.name).strip()).title() if scraped_product.name else ''
            frame_code = str(scraped_product.frame_code).strip().upper() if scraped_product.frame_code else ''
            frame_color = str(str(scraped_product.frame_color).strip()).title() if scraped_product.frame_color else ''
            lens_code = str(scraped_product.lens_code).strip().upper() if scraped_product.lens_code else ''
            lens_color = str(str(scraped_product.lens_color).strip()).title() if scraped_product.lens_color else ''
            status = str(str(scraped_product.status).strip()).lower() if scraped_product.status else 'active'
            type = str(str(scraped_product.type).strip()).title() if scraped_product.type else ''
            created_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            url = str(scraped_product.url).strip() if scraped_product.url else ''
            
            query = 'INSERT INTO products (brand_id, number, name, frame_code, frame_color, lens_code, lens_color, status, type, url, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            values = [brand_id, number, name, frame_code, frame_color, lens_code, lens_color, status, type, url, created_at, updated_at]
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query, values)
            if cursor:
                new_inserted_product_id = cursor.lastrowid
                cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in insert_product: {e}')
            else: pass
        finally: return new_inserted_product_id
    

    # metafields
    # get metafield from database against product_id
    def get_metafield_by_product_id(self,product_id: int) -> Metafields:
        metafields = Metafields()
        try:
            query = f'SELECT * FROM metafields WHERE product_id = {product_id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: 
                metafield_data = cursor.fetchone()
                cursor.close()
                if metafield_data:
                    metafields.id = metafield_data['id']
                    metafields.product_id = metafield_data['product_id']
                    metafields.for_who = metafield_data['for_who']
                    metafields.product_size = metafield_data['product_size']
                    metafields.activity = metafield_data['activity']
                    metafields.lens_material = metafield_data['lens_material']
                    metafields.graduabile = metafield_data['graduabile']
                    metafields.interest = metafield_data['interest']
                    metafields.lens_technology = metafield_data['lens_technology']
                    metafields.frame_material = metafield_data['frame_material']
                    metafields.frame_shape = metafield_data['frame_shape']
                    metafields.gtin1 = metafield_data['gtin1']
                    metafields.img_url = metafield_data['img_url']
                    if str(metafield_data['img_360_urls']).strip():
                        for img_360 in str(metafield_data['img_360_urls']).strip().split(','): 
                            metafields.img_360_urls = img_360
                    else: metafields.img_360_urls
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_metafield: {e}')
            else: pass
        finally: return metafields

    # get all metafields from database against brand_id
    def get_metafields_by_brand_id(self,brand_id: int) -> list[Metafields]:
        metafields = []
        try:
            query = f'SELECT metafields.* FROM metafields INNER JOIN products ON metafields.product_id = products.id WHERE products.brand_id = {brand_id} ORDER BY metafields.product_id;'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for metafield_data in cursor.fetchall():
                metafield = Metafields()
                metafield.id = metafield_data['id']
                metafield.product_id = metafield_data['product_id']
                metafield.for_who = metafield_data['for_who']
                metafield.product_size = metafield_data['product_size']
                metafield.activity = metafield_data['activity']
                metafield.lens_material = metafield_data['lens_material']
                metafield.graduabile = metafield_data['graduabile']
                metafield.interest = metafield_data['interest']
                metafield.lens_technology = metafield_data['lens_technology']
                metafield.frame_material = metafield_data['frame_material']
                metafield.frame_shape = metafield_data['frame_shape']
                metafield.gtin1 = metafield_data['gtin1']
                metafield.img_url = metafield_data['img_url']
                for img_360 in str(metafield_data['img_360_urls']).strip().split(','):
                    if str(img_360).strip(): metafield.img_360_urls = str(img_360).strip()
                metafields.append(metafield)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_metafields: {e}')
            else: pass
        finally: return metafields

    # update for_who metafield against metafield id
    def update_for_who_metafield(self,for_who: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET for_who = "{for_who}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_for_who_metafield: {e}')
            else: pass
    
    # update product_size metafield against metafield id
    def update_product_size_metafield(self,product_size: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET product_size = "{product_size}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_product_size_metafield: {e}')
            else: pass

    # update activity metafield against metafield id
    def update_activity_metafield(self,activity: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET activity = "{activity}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_activity_metafield: {e}')
            else: pass

    # update lens_material metafield against metafield id
    def update_lens_material_metafield(self,lens_material: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET lens_material = "{lens_material}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_lens_material_metafield: {e}')
            else: pass

    # update graduabile metafield against metafield id
    def update_graduabile_metafield(self,graduabile: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET graduabile = "{graduabile}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_graduabile_metafield: {e}')
            else: pass

    # update interest metafield against metafield id
    def update_interest_metafield(self,interest: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET interest = "{interest}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_interest_metafield: {e}')
            else: pass

    # update lens_technology metafield against metafield id
    def update_lens_technology_metafield(self,lens_technology: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET lens_technology = "{lens_technology}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_lens_technology_metafield: {e}')
            else: pass

    # update frame_material metafield against metafield id
    def update_frame_material_metafield(self,frame_material: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET frame_material = "{frame_material}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_frame_material_metafield: {e}')
            else: pass

    # update frame_shape metafield against metafield id
    def update_frame_shape_metafield(self,frame_shape: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET frame_shape = "{frame_shape}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_frame_shape_metafield: {e}')
            else: pass

    # update gtin1 metafield against metafield id
    def update_gtin1_metafield(self,gtin1: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET gtin1 = "{gtin1}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_gtin1_metafield: {e}')
            else: pass

    # update img_url metafield against metafield id
    def update_img_url_metafield(self,img_url: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET img_url = "{img_url}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_img_url_metafield: {e}')
            else: pass

    # update img_360_urls metafield against metafield id
    def update_img_360_urls_metafield(self,img_360_urls: str, id: int) -> None:
        try:
            query = f'UPDATE metafields SET img_360_urls = "{img_360_urls}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_img_360_urls_metafield: {e}')
            else: pass

    # insert new metafield #
    def insert_metafield(self,product_id: int, scraped_metafield: Metafields):
        try:
            for_who = str(scraped_metafield.for_who).strip().title() if str(scraped_metafield.for_who).strip() and 'property object at' not in str(scraped_metafield.for_who).strip().lower() else ''
            if for_who == 'Male': for_who = 'Men'
            elif for_who == 'Female': for_who = 'Women'
            product_size = str(scraped_metafield.product_size).strip() if str(scraped_metafield.product_size).strip() and 'property object at' not in str(scraped_metafield.product_size).strip().lower() else ''
            if product_size and product_size[-1] == ',': product_size = product_size[0:-1]
            activity = str(scraped_metafield.activity).strip().title() if str(scraped_metafield.activity).strip() and 'property object at' not in str(scraped_metafield.activity).strip().lower() else '' 
            lens_material = str(scraped_metafield.lens_material).strip().title() if str(scraped_metafield.lens_material).strip() and 'property object at' not in str(scraped_metafield.lens_material).strip().lower() else '' 
            graduabile = str(scraped_metafield.graduabile).strip().title() if str(scraped_metafield.graduabile).strip() and 'property object at' not in str(scraped_metafield.graduabile).strip().lower() else '' 
            interest = str(scraped_metafield.interest).strip() if str(scraped_metafield.interest).strip() and 'property object at' not in str(scraped_metafield.interest).strip().lower() else ''
            lens_technology = str(scraped_metafield.lens_technology).strip() if str(scraped_metafield.lens_technology).strip() and 'property object at' not in str(scraped_metafield.lens_technology).strip().lower() else ''
            frame_material = str(scraped_metafield.frame_material).strip() if str(scraped_metafield.frame_material).strip() and 'property object at' not in str(scraped_metafield.frame_material).strip().lower() else ''
            frame_shape = str(scraped_metafield.frame_shape).strip() if str(scraped_metafield.frame_shape).strip() and 'property object at' not in str(scraped_metafield.frame_shape).strip().lower() else ''
            gtin1 = str(scraped_metafield.gtin1).strip() if str(scraped_metafield.gtin1).strip() and 'property object at' not in str(scraped_metafield.gtin1).strip().lower() else ''
            if gtin1 and gtin1[-1] == ',': gtin1 = gtin1[0:-1]
            img_url = str(scraped_metafield.img_url).strip() if str(scraped_metafield.img_url).strip() and 'property object at' not in str(scraped_metafield.img_url).strip().lower() else ''
            imgs_360 = ''
            for i in scraped_metafield.img_360_urls:
                imgs_360 += f'{i}, '

            if str(imgs_360).strip() and str(imgs_360).strip()[-1] == ',': imgs_360 = str(imgs_360).strip()[0:-1]

            query = 'INSERT INTO metafields (product_id, for_who, product_size, activity, lens_material, graduabile, interest, lens_technology, frame_material, frame_shape, gtin1, img_url, img_360_urls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            values = [product_id, for_who, product_size, activity, lens_material, graduabile, interest, lens_technology, frame_material, frame_shape, gtin1, img_url, imgs_360]
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query, values)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in insert_metafield: {e}')
            else: pass

    # variants
    # get all variants from database against product_id
    def get_variants_by_product_id(self,product_id: int) -> list[Variant]:
        variants = []
        try:
            query = f'SELECT * FROM variants WHERE product_id = {product_id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: 
                variants_data = cursor.fetchall()
                cursor.close()

                for variant_data in variants_data:
                    variant = Variant()
                    variant.id = variant_data['id']
                    variant.product_id = variant_data['product_id']
                    variant.position = variant_data['position']
                    variant.title = variant_data['title']
                    variant.sku = variant_data['sku']
                    variant.inventory_quantity = variant_data['inventory_quantity']
                    variant.found_status = variant_data['found_status']
                    variant.listing_price = variant_data['price']
                    variant.barcode_or_gtin = variant_data['barcode_or_gtin']
                    variant.size = variant_data['size']
                    variant.weight = variant_data['weight']
                    variant.shopify_id = variant_data['shopify_id']
                    variant.inventory_item_id = variant_data['inventory_item_id']
                    variants.append(variant)
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_variants_by_product_id: {e}')
            else: pass
        finally: return variants

    # get all variants from database against brand_id
    def get_variants_by_brand_id(self,brand_id: int) -> list[Variant]:
        variants = []
        try:
            query = f'SELECT variants.* FROM variants INNER JOIN products ON variants.product_id = products.id WHERE products.brand_id = {brand_id} ORDER BY product_id, position'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            variants_data = cursor.fetchall()
            for variant_data in variants_data:
                variant = Variant()
                variant.id = variant_data['id']
                variant.product_id = variant_data['product_id']
                variant.position = variant_data['position']
                variant.title = variant_data['title']
                variant.sku = variant_data['sku']
                variant.inventory_quantity = variant_data['inventory_quantity']
                variant.found_status = variant_data['found_status']
                variant.listing_price = variant_data['price']
                variant.barcode_or_gtin = variant_data['barcode_or_gtin']
                variant.size = variant_data['size']
                variant.weight = variant_data['weight']
                variant.shopify_id = variant_data['shopify_id']
                variant.inventory_item_id = variant_data['inventory_item_id']
                variants.append(variant)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_variants_by_brand_id: {e}')
            else: pass
        finally: return variants

    # get all variants from database against product_id and created_time
    def get_variants_by_product_id_and_created_time(self,product_id: int, created_time: str) -> list[Variant]:
        variants = []
        try:
            query = f'SELECT * FROM variants WHERE product_id = {product_id} AND created_at NOT LIKE "{created_time}%"'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            variants_data = cursor.fetchall()
            for variant_data in variants_data:
                variant = Variant()
                variant.id = variant_data['id']
                variant.product_id = variant_data['product_id']
                variant.position = variant_data['position']
                variant.title = variant_data['title']
                variant.sku = variant_data['sku']
                variant.inventory_quantity = variant_data['inventory_quantity']
                variant.found_status = variant_data['found_status']
                variant.listing_price = variant_data['price']
                variant.barcode_or_gtin = variant_data['barcode_or_gtin']
                variant.size = variant_data['size']
                variant.weight = variant_data['weight']
                variant.shopify_id = variant_data['shopify_id']
                variant.inventory_item_id = variant_data['inventory_item_id']
                variants.append(variant)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_variants_by_product_id_and_created_time: {e}')
            else: pass
        finally: return variants

    # update variant inventory qunatity against variant id
    def update_variant_inventory_quantity(self,inventory_quantity: int, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET inventory_quantity = {inventory_quantity}, updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_inventory_quantity: {e}')
            else: pass

    # update variant sku against variant id
    def update_variant_sku(self, sku: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET sku = "{sku}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_sku: {e}')
            else: pass
    
    # update variant found status against variant id
    def update_variant_found_status(self,found_status: int, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET found_status = {found_status}, updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_found_status: {e}')
            else: pass

    # update variant price against variant id
    def update_variant_price(self,price: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET price = "{price}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_price: {e}')
            else: pass

    # update variant barcode against variant id
    def update_variant_barcode(self,barcode: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET barcode_or_gtin = "{barcode}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_barcode: {e}')
            else: pass

    # update variant size against variant id
    def update_variant_size(self,size: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET size = "{size}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_size: {e}')
            else: pass
    
    # update shopify_id against variant id
    def update_variant_shopify_id(self,shopify_id: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET shopify_id = "{shopify_id}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_shopify_id: {e}')
            else: pass

    # update inventory_item_id against variant id
    def update_variant_inventory_item_id(self,inventory_item_id: str, id: int) -> None:
        try:
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            query = f'UPDATE variants SET inventory_item_id = "{inventory_item_id}", updated_at = "{updated_at}" WHERE id = {id}'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in update_variant_inventory_item_id: {e}')
            else: pass

    # insert new variant #
    def insert_variant(self,product_id: int, position: int, scraped_variant: Variant):
        try:
            variant_title = str(scraped_variant.title) if scraped_variant.title else ''
            sku = str(scraped_variant.sku).strip().upper() if scraped_variant.sku else ''
            inventory_quantity = int(scraped_variant.inventory_quantity) if scraped_variant.inventory_quantity else 0
            found_status = int(scraped_variant.found_status) if scraped_variant.found_status else 0
            price = str(scraped_variant.listing_price).strip() if scraped_variant.listing_price else ''
            barcode_or_gtin = str(scraped_variant.barcode_or_gtin).strip() if scraped_variant.barcode_or_gtin else ''
            created_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            updated_at = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            size = str(scraped_variant.size).strip()
            weight = str(scraped_variant.weight).strip() if scraped_variant.weight else '0.5'

            query = 'INSERT INTO variants (product_id, position, title, sku, inventory_quantity, found_status, price, barcode_or_gtin, size, created_at, updated_at, weight) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            values = [product_id, position, variant_title, sku, inventory_quantity, found_status, price, barcode_or_gtin, size, created_at, updated_at, weight]
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query, values)
            if cursor: cursor.close()
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in insert_variant: {e}')
            else: pass

    # safilo lens code and colors
    # get metafield from database against product_id
    def get_lens_color_against_code(self, lens_code: str) -> str:
        lens_color = ''
        try:
            query = f'SELECT * FROM safilo_lens_code_colors WHERE lens_code = "{lens_code}"'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            if cursor: 
                data = cursor.fetchone()
                cursor.close()
                if data:
                    lens_color = data['lens_color']
            if db: db.commit()
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_lens_color_against_code: {e}')
            else: pass
        finally: return lens_color




    def get_product_type_of_brand_products(self, brand_id: int) -> list[str]:
        product_types = []
        try:
            query = f'SELECT * FROM products WHERE brand_id = {brand_id} GROUP BY type'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for product in cursor.fetchall():
                product_type = str(product['type']).strip()
                if product_type:
                    product_types.append(product_type)
            if cursor: cursor.close()
            if db: db.commit()
            
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_type_of_brand_products: {e}')
            else: pass
        finally: return product_types

    def get_product_types_by_store_id(self, store_id: int) -> list[str]:
        product_types = []
        try:
            query = f'SELECT product_types.* FROM `store_product_types` INNER JOIN stores ON store_product_types.store_id = stores.id INNER JOIN product_types ON store_product_types.product_type_id = product_types.id WHERE stores.id = {store_id} ORDER BY product_types.id'
            if not self.db_connection_obj: self.get_db_obj()
            db = self.db_connection_obj.get_db_connection()
            cursor = db.cursor()
            cursor.execute(query)
            for product in cursor.fetchall():
                product_types.append(str(product['name']).strip().title())
            if cursor: cursor.close()
            if db: db.commit()
            
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_types_by_store_id: {e}')
            else: pass
        finally: return product_types