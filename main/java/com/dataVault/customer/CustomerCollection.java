package com.dataVault.customer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class CustomerCollection {
    /*
    This class processes the newly added customer collection changes events from a mongodb collection called customer
    * */
    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("customerCollection").master("local[1]").getOrCreate();

        Dataset<Row> customers = session.read().json("in/customer_collection.json");

        Dataset<Row> distinct_customers = customers.select("_id").distinct().filter("_id is not null");

        Dataset<Row> existing_customers = session.read().parquet("out/hub_customer/*/*/*/*/*/*/");

        distinct_customers.registerTempTable("distinct_customers");

        existing_customers.registerTempTable("existing_customers");

        Dataset<Row> customers_to_update = session.sql("SELECT _id " +
                                                  "FROM distinct_customers " +
                                                  "WHERE _id NOT IN (SELECT customer_internal_application_id FROM existing_customers)");

        session.udf().register("getMd5Hash", (String x) -> getMd5Hash(x), DataTypes.StringType);

        Dataset<Row> customer_hub_data = customers_to_update.withColumn("customer_hashkey", callUDF("getMd5Hash", col("_id")))
                                                           .withColumn("customer_internal_application_id", col("_id"))
                                                           .withColumn("record_source", lit("app_customer_collection"))
                                                           .withColumn("created_at", current_timestamp() )
                                                           .drop("_id");

        String date = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss").format(new Date());

        customer_hub_data.repartition(1).write().mode("overwrite").parquet("out/hub_customer/" + date);

        Dataset<Row> check_count = session.read().parquet("out/hub_customer/*/*/*/*/*/*/");

        System.out.println(check_count.count());

    }

    private static String getMd5Hash(String business_key) throws Exception{
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(business_key.getBytes());
        byte[] digest = md.digest();
        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}
