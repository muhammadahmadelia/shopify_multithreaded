from modules.files_reader import Files_Reader
from modules.query_processor import Query_Processor

DEBUG = True
connection_file = 'files/conn.txt'

file_reader = Files_Reader(DEBUG)
conn_string = file_reader.read_text_file(connection_file)

# getting all stores from database
query_processor = Query_Processor(DEBUG, conn_string)

single_products = []
stores = query_processor.get_stores()
for store in stores:
    if store.id == 2:
        for brand in query_processor.get_brands_by_store_id(store.id):
        
            for product in query_processor.get_products_by_brand_id(brand.id):
                new_pro = [product.number, product.name, product.frame_code, product.lens_code]

                if new_pro not in single_products:
                    single_products.append(new_pro)
                else: print(product.id, brand.name, product.number, product.name, product.frame_code, product.frame_color, product.lens_code, product.lens_color, product.shopify_id)