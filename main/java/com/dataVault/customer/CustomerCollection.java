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
    2. adds new records to the satellite table
    3. creates new emails to the email hub
    4. adds new records to the email satellite table
    5. adds records to the customer email link
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

        Dataset<Row> sat_customer_collection_ds = customers.filter("_id is not null")
                .drop("confirmation_token", "confirmation_token_expires_at", "password_hash", "emails");

        Utils.updateSatTable(session, sat_customer_collection_ds, "_id", "customer_hash_key"
                , "sat_customer_collection", "app_customer_collection");

        customers.filter("_id is not null").registerTempTable("customers");
        Dataset<Row> emails = session.sql("SELECT _id AS customer_id, emails_e.* FROM customers LATERAL VIEW explode(emails) as emails_e");
        emails.cache();
        emails.registerTempTable("emails");

        Dataset<Row> distinct_email_ids = emails.select("_id").distinct();

        Utils.updateHubTable(session, distinct_email_ids, "hub_email", "_id"
                , "email_internal_application_id", "app_customer_collection"
                ,  "email_hash_key");

        Dataset<Row> sat_email_ds = emails.drop("customer_id");

        Utils.updateSatTable(session, sat_email_ds, "_id", "email_hash_key"
                , "sat_email", "app_customer_collection");

        Dataset<Row> link_customer_email_ds = session.sql("SELECT DISTINCT customer_id AS customer_hash_key, _id AS email_hash_key FROM emails");

        Utils.updateLinkTable(session, "link_customer_email", link_customer_email_ds, "customer_email_hash_key", "app_customer_collection");
    }
}