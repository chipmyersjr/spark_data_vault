CREATE VIEW dim_product
AS
SELECT
         h.product_hash_key
       , s.loaded_at AS valid_from
       , LEAD(s.loaded_at) OVER(PARTITION BY h.product_hash_key ORDER BY s.loaded_at) AS valid_to
       , s.title
       , s.product_type
       , s.description
       , s.vendor
       , s.store_id
       , s.inventory
       , s.sale_price_in_cents
       , s.created_at_ts
       , s.updated_at_ts
FROM hub_product h
LEFT JOIN sat_product_collection s ON h.product_hash_key = s.product_hash_key



CREATE VIEW dim_customer_history
AS
SELECT
         h.customer_hash_key
      ,  p.loaded_at AS valid_from
      ,  LEAD(p.loaded_at) OVER (PARTITION BY h.customer_hash_key ORDER BY p.loaded_at) AS valid_to
      ,  s1.primary_email
      ,  s2.currency
      ,  s2.first_name
      ,  s2.last_name
      ,  s2.total_spent
      ,  s2.last_cart_activity_at_ts
      ,  s2.last_cart_created_at_ts
      ,  s2.log_out_expires_at_ts
      ,  s2.confirmed_on_ts
      ,  s2.confirmation_token_expires_at_ts
      ,  s2.created_at_ts
      ,  s2.updated_at_ts
FROM pit_customer p
JOIN hub_customer h ON p.customer_hash_key = h.customer_hash_key
LEFT JOIN sat_customer_primary_email s1 ON p.customer_hash_key = s1.customer_hash_key AND p.sat_customer_primary_email_loaded_at = s1.loaded_at
LEFT JOIN sat_customer_collection s2 ON p.customer_hash_key = s2.customer_hash_key AND p.sat_customer_collection_loaded_at = s2.loaded_at



CREATE VIEW fact_cart_adds_and_drops
AS
SELECT
          l1.product_hash_key
       ,  l2.customer_hash_key
       ,  s1.quantity_added
       ,  s1.quantity_dropped
       ,  s1.event_time
FROM sat_cart_adds_and_drops s1
JOIN link_cart_product l1 ON s1.cart_product_hash_key = l1.cart_product_hash_key
JOIN link_customer_cart l2 ON l1.cart_hash_key = l2.cart_hash_key