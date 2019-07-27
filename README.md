# spark_data_vault
This is a small example of building a Data Vault 2.0 style data warehouse using Spark. Its aim is to make a data mart for add and drops for a customer cart feature of an ecommerce style application.  The data comes from another one of my projects. A flask api with MongoDB as a data store.  All MongoDB change events are sent to Kinesis Data Stream then Kinesis Firehose is used to land all the events in an S3 bucket.  This is done by definining a post-save event through Flask's ORM. (https://github.com/chipmyersjr/flask_rest_api/tree/master/kafka_server)

All data vault table types (satellites, hubs, links, point-in-time...) are encapsulated into methods in a Utils class (https://github.com/chipmyersjr/spark_data_vault/blob/master/main/java/com/dataVault/commons/Utils.java). There is a seperate spark job for each data source which converts that data source to a spark data frame and passes it to the necessary functions to create the data vault tables.  

All data vault tables are stored in S3 in parquet format. AWS Glue crawler was used to create the data catalog for AWS Athena.  The information mart (dimensional model) is implemented as Athena views (https://github.com/chipmyersjr/spark_data_vault/blob/master/athena_view.sql)



# raw data vault:


**hub_customer:**
* customer_hash_key
* customer_internal_application_id
* record_source
* created_at

**sat_customer_collection:**
* customer_hash_key
* loaded_at
* record_source
* customer_internal_application_id
* currency
* first_name
* last_name
* total_spent
* last_order_date
* last_cart_activity_at
* last_cart_created_at
* log_out_expires_at
* confirmed_on
* confirmation_token_expires_at
* last_seen_date
* created_at
* updated_at
* deleted_at

**hub_product:**
* product_hash_key
* product_internal_application_id
* record_source
* created_at

**sat_product_collection:**
* product_hash_key
* loaded_at
* record_source
* title
* product_type
* description
* vendor
* store
* inventory
* sale_price_in_cents
* created_at
* updated_at
* deleted_at

**hub_cart:**
* cart_hash_key
* cart_internal_application_id
* record_source
* created_at

**sat_cart**
* cart_hash_key
* loaded_at
* record_source
* state
* created_at
* last_item_added_at
* invoice_created_at
* closed_at

**link_customer_cart:**
* customer_cart_hash_key
* customer_hash_key
* cart_hash_key
* created_at
* record_source

**hub_email:**
* email_hash_key
* loaded_at
* record_source
* email_internal_application_id

**sat_email:**
* email_hash_key
* loaded_at
* record_source
* email
* is_primary
* created_at
* updated_at
* deleted_at

**link_customer_email:**
* customer_email_hash_key
* loaded_at
* record_source
* customer_hash_key
* email_hash_key

**link_cart_product:**
* cart_product_hash_key
* cart_hash_key
* product_hash_key
* created_at
* record_source

**sat_cart_product:** 
* cart_product_hash_key
* loaded_at
* record_source
* quantity
* added_at
* removed_at
* invoice_created_at

# business data vault:

**sat_customer_primary_email:**
(implements business rule that all customer have one primary email at a given time)
* customer_hash_key
* loaded_at
* record_source
* primary_email

**sat_customer_pit:**
* customer_hash_key
* loaded_at
* sat_customer_collection_loaded_at
* sat_customer_primary_email_loaded_at

**sat_cart_adds_and_drops:**
(parses cart events and classifies them as either user adding a product or user dropping a product)
* cart_product_hash_key
* loaded_at
* record_source
* quantity_added
* quantity_dropped
* event_time


# information mart

**dim_product:**
* product_hash_key
* valid_from
* valid_to
* title
* product_type
* description
* vendor
* store
* inventory
* sale_price_in_cents
* created_at_ts
* updated_at_ts


**dim_customer_history:**
* customer_hash_key
* valid_from
* valid_to
* primary_email
* customer_internal_application_id
* currency
* first_name
* last_name
* total_spent
* last_order_date
* last_cart_activity_at
* last_cart_created_at
* log_out_expires_at
* confirmed_on
* confirmation_token_expires_at
* created_at
* updated_at

