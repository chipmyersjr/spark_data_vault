package com.dataVault.cart;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CartItemCollection {
    public static void main(String[] args) {
        /*
        Processes the cart item collection from mongodb

        1. updates link_cart_product
        2. updates sat_cart_product
        * */
        String filePath = "s3n://flask-app-88/cart_item/2019/07/*/*/*";

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("cartItemCollectionSC")
                .setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().appName("cartItemCollection").master("local[1]").getOrCreate();

        Dataset<Row> cart_items = Utils.getDataSetFromKinesisFirehouseS3Format(session, sc, filePath);

        String[] unixColumns = new String[] {"added_at", "removed_at"};

        cart_items = Utils.convertUnixTime(session, cart_items, unixColumns);

        cart_items.registerTempTable("cart_items");

        Dataset<Row> linkCartProduct = session.sql("SELECT DISTINCT cart_id AS cart_hash_key, product_id AS product_hash_key FROM cart_items");

        Utils.updateLinkTable(session, "link_cart_product", linkCartProduct, "cart_product_hash_key", "app_cart_item_collection");

        String[] linkTableIdColumnNames = new String[]{"cart_id", "product_id"};
        Utils.updateSatTable(session, cart_items, "cart_id_product_id_combined", "cart_product_hash_key"
                , "sat_cart_product", "app_cart_item_collection", linkTableIdColumnNames);
    }
}