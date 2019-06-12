package com.dataVault.customer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.dataVault.commons.Utils;

public class CustomerCollection {
    /*
    This class processes the newly added customer collection changes events from a mongodb collection called customer

    1. adds newly found customer ids to customer hub
    * */
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("customerCollection").master("local[1]").getOrCreate();

        Dataset<Row> customers = session.read().json("in/customer_collection.json");

        Dataset<Row> distinct_customers = customers.select("_id").distinct().filter("_id is not null");

        Utils.updateHubTable(session, distinct_customers, "hub_customer", "_id"
                , "customer_internal_application_id", "app_customer_collection"
                ,  "customer_hash_key");
    }
}