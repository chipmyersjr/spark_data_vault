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

        Dataset<Row> check_count = session.read().parquet( "out/hub_cart/*/*/*/*/*/*/");

        check_count.count();
        check_count.show();
    }
}
