package com.dataVault.commons;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class Utils {

    private static final String outPath = "out/"; //"s3n://chip-data-vault/raw-data-vault/";

    public static void updateHubTable(SparkSession session, Dataset<Row> newIdsDataset, String hubTableName
                                      , String idColumnName, String businessKeyColumnName, String recordSource
                                      , String hashKeyColumnName) {
        /*
        Generic function to update a hub table given a distinct list of business keys.  Will insert new hub record for
        all keys that don't currently exist in the hub tables

        session: SparkSession object to perform operations
        newIdsDataset: Distinct list of newly arrived business keys
        hubTableName: the name of the hub table
        idColumnName: column name of the key column in newIdsDataset
        businessKeyColumnName: business key column name for hub table
        recordSource: record source field value for hub table
        hashKeyColumnName: the name of the hash key column in the hub table
        * */

        Dataset<Row> ids_to_update;
        String hub_dir = outPath + hubTableName;
        File dir = new File(hub_dir);

        if (dir.exists()){
            Dataset<Row> existing_ids = session.read().parquet(hub_dir + "/*/*/*/*/*/*/");

            newIdsDataset.registerTempTable("new");

            existing_ids.registerTempTable("existing");

            ids_to_update = session.sql("SELECT " + idColumnName +
                    " FROM new " +
                    " WHERE + " + idColumnName +
                    " NOT IN (SELECT " +  businessKeyColumnName + " FROM existing)");
        } else {
            ids_to_update = newIdsDataset.select(idColumnName);
        }

        session.udf().register("getMd5Hash", (String x) -> getMd5Hash(x), DataTypes.StringType);

        Dataset<Row> hub_data = ids_to_update.withColumn(hashKeyColumnName, callUDF("getMd5Hash", col(idColumnName)))
                .withColumn(businessKeyColumnName, col(idColumnName))
                .withColumn("record_source", lit(recordSource))
                .withColumn("created_at", current_timestamp() )
                .drop(idColumnName);

        String date = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss").format(new Date());

        hub_data.repartition(1).write().mode("overwrite").parquet(hub_dir + "/" + date);
    }

    public static void updateSatTable(SparkSession session, Dataset<Row> ds, String idColumnName, String hashKeyColumnName
                                       , String satelliteTableName, String recordSource){
        /*
        generic function for inserting new records into a satellite table

        session: SparkSession object to perform operations
        ds: new records received to add to satellite table
        idColumnName: column name of the key column in ds
        hashKeyColumnName: hash key column name. will match corresponding hub or link hash
        satelliteTableName: name of the satellite table
        recordSource: record source field value for satellite table
        * */
        String sat_dir = outPath + satelliteTableName;

        session.udf().register("getMd5Hash", (String x) -> getMd5Hash(x), DataTypes.StringType);

        Dataset<Row> satellite = ds.withColumn(hashKeyColumnName, callUDF("getMd5Hash", col(idColumnName)))
                .withColumn("loaded_at", current_timestamp())
                .withColumn("record_source", lit(recordSource));

        String date = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss").format(new Date());

        satellite.repartition(1).write().mode("overwrite").parquet(sat_dir + "/" + date);
    }

    private static String getMd5Hash(String business_key) throws Exception {
        /*
        Returns MD5 hash for given business_key

        business_key: business key to hash
        * */
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(business_key.getBytes());
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}
