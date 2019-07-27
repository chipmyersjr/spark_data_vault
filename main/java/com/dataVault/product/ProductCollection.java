package com.dataVault.product;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

        String filePath = "s3n://flask-app-88/product/2019/07/*/*/*";

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("productCollectionSC")
                .setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().appName("productCollection").master("local[1]").getOrCreate();

        Dataset<Row> products = Utils.getDataSetFromKinesisFirehouseS3Format(session, sc, filePath);

        String[] unixColumns = new String[] {"created_at", "updated_at"};

        products = Utils.convertUnixTime(session, products, unixColumns);

        Dataset<Row> distinct_products = products.select("_id").distinct().filter("_id is not null");

        Utils.updateHubTable(session, distinct_products, "hub_product", "_id"
                , "product_internal_application_id", "app_product_collection"
                ,  "product_hash_key");

        Dataset<Row> sat_product_collection_ds = products.filter("_id is not null").drop("tags", "_corrupt_record");

        Utils.updateSatTable(session, sat_product_collection_ds, "_id", "product_hash_key"
                , "sat_product_collection", "app_product_collection");

    }
}
