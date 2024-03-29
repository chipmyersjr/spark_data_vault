package com.dataVault.commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class Utils {

    private static final String outPath = "s3n://chip-data-vault-west2/data-vault/";

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
        implements hash_diff column and only adds new history records if the hash_diff has changed from the most recent record

        session: SparkSession object to perform operations
        ds: new records received to add to satellite table
        idColumnName: column name of the key column in ds
        hashKeyColumnName: hash key column name. will match corresponding hub or link hash
        satelliteTableName: name of the satellite table
        recordSource: record source field value for satellite table
        * */

        String sat_dir = outPath + satelliteTableName;
        Dataset<Row> recordsForUpdate;

        boolean first = true;
        String hashDiffColumnName = "hash_diff";
        String [] columnNames = ds.columns();
        Arrays.sort(columnNames);

        ds = ds.distinct();

        for (String columnName : columnNames) {
            if (first) {
                ds = ds.withColumn(hashDiffColumnName, coalesce(col(columnName).cast("string"), lit(" ")));
                first = false;
            } else {
                ds = ds.withColumn(hashDiffColumnName, concat(col(hashDiffColumnName), lit("|"), coalesce(col(columnName).cast("string"), lit(" "))));
            }
        }

        session.udf().register("getMd5Hash", (String x) -> getMd5Hash(x), DataTypes.StringType);

        ds = ds.withColumn(hashKeyColumnName, callUDF("getMd5Hash", col(idColumnName)))
                .withColumn("loaded_at", current_timestamp())
                .withColumn("record_source", lit(recordSource))
                .withColumn(hashDiffColumnName, callUDF("getMd5Hash", col(hashDiffColumnName)));

        File dir = new File(sat_dir);

        if (dir.exists()){
            session.read().parquet(sat_dir + "/*/*/*/*/*/*/").registerTempTable("satellite");

            String query_template = "SELECT %s AS hash_key, %s AS check_hash_diff FROM " +
                                    "(SELECT %s, %s, ROW_NUMBER() OVER(PARTITION BY %s ORDER BY loaded_at DESC) AS row_num " +
                                    "FROM satellite) a " +
                                    "WHERE a.row_num = 1";
            String query = String.format(query_template, hashKeyColumnName, hashDiffColumnName, hashKeyColumnName, hashDiffColumnName, hashKeyColumnName);

            session.sql(query).registerTempTable("existing");

            ds.registerTempTable("new");


            query_template = "SELECT * " +
                             "FROM new " +
                             "LEFT JOIN existing ON hash_key = %s " +
                             "WHERE check_hash_diff IS NULL OR check_hash_diff <> %s";
            query = String.format(query_template, hashKeyColumnName, hashDiffColumnName);

            recordsForUpdate = session.sql(query).drop("hash_key", "check_hash_diff");
        } else {
            recordsForUpdate = ds;
        }

        if (recordsForUpdate.count() > 0) {
            String date = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss").format(new Date());

            recordsForUpdate.repartition(1).write().mode("overwrite").parquet(sat_dir + "/" + date);
        }
    }

    public static void updateLinkTable(SparkSession session, String linkTableName, Dataset<Row> ds, String linkHashKeyName
                                        , String recordSource) {
        /*
        generic function for inserting new rows into a link table. implements hash key generation for link tables as hash of combination
        of business keys then converts business keys to the hash value

        session: SparkSession object to perform operations
        linkTableName: name to be given to the link table. column names should be given as you want them to appear in the link table
        ds: new records to be added to the link table
        linkHashKeyName: name to be given to the hash key of the link table
        recordSource: record source field value for link table
        * */
        String link_dir = outPath + linkTableName;
        Dataset<Row> newRecords;

        session.udf().register("getMd5Hash", (String x) -> getMd5Hash(x), DataTypes.StringType);

        String[] columnNames = ds.columns();

        boolean first = true;
        for (String columnName : columnNames) {
            if (first) {
                ds = ds.withColumn(linkHashKeyName, col(columnName));
                first = false;
            } else {
                ds = ds.withColumn(linkHashKeyName, concat(col(linkHashKeyName), lit("|"), col(columnName)));
            }

            ds = ds.withColumn(columnName, callUDF("getMd5Hash", col(columnName)));
        }

        File dir = new File(link_dir);

        ds = ds.withColumn(linkHashKeyName, callUDF("getMd5Hash", col(linkHashKeyName)))
                .withColumn("loaded_at", current_timestamp())
                .withColumn("record_source", lit(recordSource));

        if (dir.exists()) {
            Dataset<Row> existing = session.read().parquet(link_dir + "/*/*/*/*/*/*/");

            ds.registerTempTable("new");

            existing.registerTempTable("existing");

            newRecords = session.sql("SELECT * " +
                    " FROM new " +
                    " WHERE + " + linkHashKeyName +
                    " NOT IN (SELECT " +  linkHashKeyName + " FROM existing)");
        } else {
            newRecords = ds;
        }


        String date = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss").format(new Date());

        newRecords.repartition(1).write().mode("overwrite").parquet(link_dir + "/" + date);
    }

    public static void updateSatTable(SparkSession session, Dataset<Row> ds, String idColumnName, String hashKeyColumnName
            , String satelliteTableName, String recordSource, String[] linkTableIdColumnNames) {
        /*
        this is an overloaded version of updateSatTable created to support the building of satellite tables for link tables.
        you can now pass an array of business key names to be used for the satellite hash key instead of just  single column.
        the function creates a column that is the concatenation of the id columns and then passes the arguments to the standard
        updateSatTable function

        session: SparkSession object to perform operations
        ds: new records received to add to satellite table
        idColumnName: this will be the column name given to the concatenation of link table ids
        hashKeyColumnName: hash key column name. will match corresponding hub or link hash
        satelliteTableName: name of the satellite table
        recordSource: record source field value for satellite table
        * */

        boolean first = true;
        for (String columnName : linkTableIdColumnNames) {
            if (first) {
                ds = ds.withColumn(idColumnName, col(columnName));
                first = false;
            } else {
                ds = ds.withColumn(idColumnName, concat(col(idColumnName), lit("|"), col(columnName)));
            }
        }

        updateSatTable(session, ds, idColumnName, hashKeyColumnName, satelliteTableName, recordSource);

    }

    public static void refreshPIT(SparkSession session, String[] satelliteNames, String hashKeyColumnName, String pitTableName) {
        /*
        Truncate and load style implementation of a point in time table.

        session: SparkSession object to perform operations
        satelliteNames: array of satellite table names to include
        hashKeyColumnName: name of the hash key used in the satellite tables
        pitTableName: table name to be given to the created point in time table
        * */

        String pit_dir = outPath + pitTableName;

        session.read().parquet( outPath + satelliteNames[0] + "/*/*/*/*/*/*/").registerTempTable(satelliteNames[0]);
        Dataset<Row> loadDates = session.sql("SELECT loaded_at, " + hashKeyColumnName + " FROM " + satelliteNames[0]);

        StringBuilder selectClause = new StringBuilder("SELECT ");
        StringBuilder fromClause = new StringBuilder(" FROM load_dates l");

        selectClause.append("l.").append(hashKeyColumnName).append(", l.loaded_at");

        boolean first = true;
        int counter = 1;
        String alias;
        String satHaskCol;
        String lHashCol;
        for (String name : satelliteNames) {
            alias = "s" + counter;
            satHaskCol = alias + "." + hashKeyColumnName;
            lHashCol = "l." + hashKeyColumnName;
            fromClause.append(" LEFT JOIN ").append(name).append(" ").append(alias).append(" ON ").append(satHaskCol).append(" = ").append(lHashCol);

            fromClause.append(" AND ").append("l.loaded_at = ").append(alias).append(".loaded_at ");
            selectClause.append(", ").append(" MAX(").append(alias).append(".loaded_at) ");
            selectClause.append(" OVER (PARTITION BY ").append(lHashCol).append(" ORDER BY l.loaded_at) ").append(name).append("_loaded_at");

            counter += 1;
            if (first) {
                first = false;
                continue;
            }
            session.read().parquet( outPath + name + "/*/*/*/*/*/*/").registerTempTable(name);
            loadDates = loadDates.union(session.sql("SELECT loaded_at, " + hashKeyColumnName + " FROM " + name));
        }

        loadDates.registerTempTable("load_dates");
        session.sql(selectClause.append(fromClause).toString()).write().mode("overwrite").parquet(pit_dir + "/");
    }

    public static Dataset<Row> getDataSetFromKinesisFirehouseS3Format(SparkSession session, JavaSparkContext sc, String filePath) {
        /*
        parses S3 files produced by Kinesis Firehouse and returns a spark dataset

        session: SparkSession object to perform operations
        sc: SparkContext object to perform operations
        filePath: s3 location of file
         */
        Configuration hadoopConf = sc.hadoopConfiguration();

        hadoopConf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
        hadoopConf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"));

        JavaPairRDD<String, String> PairRDD = sc.wholeTextFiles(filePath);
        JavaRDD<String> RDD = PairRDD.map(x -> x._2);
        String s3_file = PairRDD.map(x -> x._1).collect().get(0);

        JavaRDD validJsonRDD = RDD.flatMap(x -> Arrays.asList(x.split("\n")).iterator());

        Dataset<Row> result = session.read().option("wholeFile", true).option("mode", "PERMISSIVE").json(validJsonRDD).withColumn("s3_location", lit(s3_file));

        return result;

    }

    public static Dataset<Row> convertUnixTime(SparkSession session, Dataset<Row> ds, String[] columns){
        /*
        convert unix timestamps into regular timestamps

        session: SparkSession object to perform operations
        ds: data set to be converted
        columns: array of column names to be converted
         */
        ds.registerTempTable("unix_ds");
        for (String column : columns) {
            if (Arrays.asList(ds.columns()).contains(column)){
                ds = session.sql(String.format("SELECT *, to_date(FROM_UNIXTIME(%s['$date'] - 1562173938000, 'YYYY-MM-dd HH:mm:ss'))  as `%s_ts` FROM unix_ds", column, column));
                ds = ds.drop(column);
                ds.registerTempTable("unix_ds");
            }
        }

        return ds;
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
