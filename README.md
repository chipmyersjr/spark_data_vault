# spark_data_vault



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
* email
* first_name
* last_name
* total_spent
* last_order_date
* last_cart_activity_at
* last_cart_created_at
* log_out_expires_at
* emails
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
