package com.dataVault.cart;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CartCollection {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("cartCollection").master("local[1]").getOrCreate();

        Dataset<Row> carts = session.read().json("in/cart_collection.json");

        Dataset<Row> distinct_carts = carts.select("_id").distinct().filter("_id is not null");

        Utils.updateHubTable(session, distinct_carts, "hub_cart", "_id"
                , "cart_internal_application_id", "app_cart_collection"
                ,  "cart_hash_key");

        Dataset<Row> sat_cart_collection_ds = carts.filter("_id is not null")
                .drop("customer_id");

        Utils.updateSatTable(session, sat_cart_collection_ds, "_id", "cart_hash_key"
                , "sat_cart_collection", "app_cart_collection");

        carts.select("_id", "customer_id").filter("_id is not null").registerTempTable("distinct_carts");

        Dataset<Row> linkCustomerCart = session.sql("SELECT _id AS cart_hash_key, customer_id AS customer_hash_key FROM distinct_carts");

        Utils.updateLinkTable(session, "link_customer_cart", linkCustomerCart, "customer_cart_hash_key", "app_cart_collection");

    }
}
