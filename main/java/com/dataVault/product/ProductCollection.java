package com.dataVault.product;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProductCollection {
    public static void main(String[] args) {
        /*
        This class processes the newly added product collection changes events from a mongodb collection called product

        1. adds newly found product ids to product hub
        2. adds new records to the satellite table
         */

        //System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("productCollection").getOrCreate();

        Dataset<Row> products = session.read().json("s3n://chip-data-vault/in/product_collection.json");

        Dataset<Row> distinct_products = products.select("_id").distinct().filter("_id is not null");

        Utils.updateHubTable(session, distinct_products, "hub_product", "_id"
                , "product_internal_application_id", "app_product_collection"
                ,  "product_hash_key");

        Dataset<Row> sat_product_collection_ds = products.filter("_id is not null");

        Utils.updateSatTable(session, sat_product_collection_ds, "_id", "product_hash_key"
                , "sat_product_collection", "app_product_collection");
    }
}
