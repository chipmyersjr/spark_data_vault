package com.dataVault.customer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

        String filePath = "s3n://flask-app-88/customer/2019/07/21/*/*";
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("customerCollectionSC")
                .setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder()
                .appName("customerCollection")
                .master("local[1]")
                .config("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
                .config("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"))
                .getOrCreate();

        Dataset<Row> customers = Utils.getDataSetFromKinesisFirehouseS3Format(session, sc, filePath);

        String[] unixColumns = new String[] {"last_seen_date", "confirmed_on", "created_at", "log_out_expires_at", "confirmation_token_expires_at", "updated_at"};

        customers = Utils.convertUnixTime(session, customers, unixColumns);

        Dataset<Row> distinct_customers = customers.select("_id").distinct().filter("_id is not null");

        Utils.updateHubTable(session, distinct_customers, "hub_customer", "_id"
                , "customer_internal_application_id", "app_customer_collection"
                ,  "customer_hash_key");

        Dataset<Row> sat_customer_collection_ds = customers.filter("_id is not null")
                .drop("confirmation_token", "confirmation_token_expires_at", "password_hash", "emails", "_corrupt_record");

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

        String[] emailUnixColumns = new String[] {"created_at"};

        sat_email_ds = Utils.convertUnixTime(session, sat_email_ds, emailUnixColumns);

        Utils.updateSatTable(session, sat_email_ds, "_id", "email_hash_key"
                , "sat_email", "app_customer_collection");

        Dataset<Row> link_customer_email_ds = session.sql("SELECT DISTINCT customer_id AS customer_hash_key, _id AS email_hash_key FROM emails");

        Utils.updateLinkTable(session, "link_customer_email", link_customer_email_ds, "customer_email_hash_key", "app_customer_collection");
    }
}