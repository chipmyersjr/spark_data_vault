package com.dataVault.cart;

import com.dataVault.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CartBusinessVault {
    public static void main(String[] args) {
        /*
        Create business vault tables related to customer

        1. updates sat_cart_adds_and_drops
        * */
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("customerBusinessVault").master("local[1]").getOrCreate();

        session.read().parquet( "out/sat_cart_product/*/*/*/*/*/*/").registerTempTable("sat_cart_product");

        //cart_product_hash_key|quantity|          removed_at|            added_at|previous_quantity|quantity_delta
        String derived_table = "(SELECT " +
                                        "cart_id_product_id_combined, " +
                                        "quantity, " +
                                        "removed_at, " +
                                        "added_at," +
                                        "LAG(quantity) OVER (PARTITION BY cart_product_hash_key ORDER BY loaded_at) AS previous_quantity,  " +
                                        "quantity - COALESCE(LAG(quantity) OVER (PARTITION BY cart_product_hash_key ORDER BY loaded_at), 0) AS quantity_delta " +
                               "FROM sat_cart_product " +
                               "ORDER BY cart_id_product_id_combined, added_at) ";

        String query = "SELECT " +
                          "cart_id_product_id_combined, " +
                          "CASE WHEN removed_at IS NULL AND quantity_delta > 0 THEN quantity_delta ELSE 0 END AS quantity_added, " +
                          "CASE WHEN removed_at IS NOT NULL THEN quantity  " +
                               "WHEN quantity_delta < 0 THEN quantity_delta * -1 " +
                               "ELSE 0 END AS quantity_dropped, " +
                          "COALESCE(removed_at, added_at) AS event_time " +
                       "FROM " + derived_table +
                       "WHERE quantity_delta <> 0 OR removed_at IS NOT NULL";

        Dataset<Row> sat_cart_adds_and_drops_ds = session.sql(query);

        Utils.updateSatTable(session, sat_cart_adds_and_drops_ds, "cart_id_product_id_combined", "cart_product_hash_key"
                , "sat_cart_adds_and_drops", "sat_cart_product");
    }
}
