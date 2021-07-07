package com.dremio.tools.iceberg.Spark;


import org.apache.iceberg.*;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import static org.apache.iceberg.types.Types.NestedField.required;

public class snapshotExpiryTest{
    static String tableDir = "/Users/plr/SparkClient/generated-tables/iceberg-table-07-10-17.870";
    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get())
    );
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("id", 16)
            .build();

    public final DataFile FILE_0 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=0")
            .withPath(new File(tableDir, "data-0.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(100)
            .build();

    public final DataFile FILE_1 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=1")
            .withPath(new File(tableDir, "data-1.parquet").toString())
            .withFileSizeInBytes(100)
            .withRecordCount(100)
            .build();

    public final DataFile FILE_2 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=2")
            .withPath(new File(tableDir, "data-2.parquet").toString())
            .withFileSizeInBytes(400)
            .withRecordCount(1000)
            .build();
    private static SparkSession spark = null;

    @BeforeClass
    public static void startSpark(){
        snapshotExpiryTest.spark = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = snapshotExpiryTest.spark;
        snapshotExpiryTest.spark = null;
        currentSpark.stop();
    }

        Table table;
    @Before
    public void setUp() {
        HadoopTables TABLES = new HadoopTables();
        table = TABLES.load(tableDir);
    }

    @Test
    public void expireOlderSnapshots() throws InterruptedException {
        table.newAppend()
                .appendFile(FILE_0)
                .appendFile(FILE_1)
                .commit();
        table.refresh();
        long count = 0;
        Iterable<Snapshot> snaps = table.snapshots();
        for (Snapshot snap : snaps) {
            count++;
        }
        Thread.sleep(1000 * 30);
        table.newAppend()
                .appendFile(FILE_2)
                .commit();
        table.refresh();
        long count1 = 0;
        Iterable<Snapshot> snaps1 = table.snapshots();
        for (Snapshot snapshot : snaps1) {
            count1++;
        }
        long tsToExpire = System.currentTimeMillis() - 1000 * 20;
        Actions act = Actions.forTable(table);
        ExpireSnapshotsActionResult exa = act.expireSnapshots().expireOlderThan(tsToExpire).execute();
        table.refresh();
        long count2 = 0;
        Iterable<Snapshot> snaps2 = table.snapshots();
        for (Snapshot snapshot : snaps2) {
            count2++;
        }
        Assert.assertEquals(1, count2);
    }
}
