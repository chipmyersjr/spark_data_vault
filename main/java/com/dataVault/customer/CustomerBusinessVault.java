package com.dataVault.customer;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomerBusinessVault {
    public static void main(String[] args) {
        /*
        Create business vault tables related to customer

        1. updates sat_customer_primary email with most recently loaded primary email per customer
        * */
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("customerBusinessVault").master("local[1]").getOrCreate();

        session.read().parquet( "out/hub_customer/*/*/*/*/*/*/").registerTempTable("hub_customer");
        session.read().parquet( "out/sat_email/*/*/*/*/*/*/").registerTempTable("sat_email");
        session.read().parquet( "out/link_customer_email/*/*/*/*/*/*/").registerTempTable("link_customer_email");

        // most recently loaded primary email per customer
        String query = "SELECT c.customer_internal_application_id, a.email AS primary_email FROM " +
                       "(SELECT *, ROW_NUMBER() OVER(PARTITION BY email_hash_key ORDER BY loaded_at DESC) AS row_num " +
                       "FROM sat_email se " +
                       "WHERE se.is_primary = true) a " +
                       "JOIN link_customer_email l ON a.email_hash_key = l.email_hash_key " +
                       "JOIN hub_customer c ON l.customer_hash_key = c.customer_hash_key " +
                       "WHERE a.row_num = 1";

        Dataset<Row> sat_customer_primary_email_ds = session.sql(query);

        sat_customer_primary_email_ds.show(100);

        Utils.updateSatTable(session, sat_customer_primary_email_ds, "customer_internal_application_id", "customer_hash_key"
                , "sat_customer_primary_email", "sat_email");

        String[] satelliteNames = new String[] {"sat_customer_collection", "sat_customer_primary_email"};

        Utils.refreshPIT(session, satelliteNames, "customer_hash_key", "pit_customer");
    }
}
