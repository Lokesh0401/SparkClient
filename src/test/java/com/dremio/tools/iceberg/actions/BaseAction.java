package com.dremio.tools.iceberg.actions;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseAction {
  static final Schema SCHEMA = new Schema(
    required(1, "id", Types.IntegerType.get()),
    required(2, "data", Types.StringType.get()));
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
    .bucket("id", 16)
    .build();
  static final String tableDir = "/Users/saurabhagarwal/Desktop/Repositories/iceberg-generator/generated-tables";
  private static SparkSession sparkSession = null;
  final DataFile FILE_0 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=0")
    .withPath(new File(tableDir, "data-0.parquet").toString())
    .withFileSizeInBytes(10)
    .withRecordCount(100)
    .build();
  final DataFile FILE_1 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=1")
    .withPath(new File(tableDir, "data-1.parquet").toString())
    .withFileSizeInBytes(100)
    .withRecordCount(100)
    .build();
  final DataFile FILE_2 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=2")
    .withPath(new File(tableDir, "data-2.parquet").toString())
    .withFileSizeInBytes(400)
    .withRecordCount(1000)
    .build();
  final DataFile FILE_3 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=3")
    .withPath(new File(tableDir, "data-3.parquet").toString())
    .withFileSizeInBytes(400)
    .withRecordCount(1000)
    .build();
  Table table;

  @BeforeClass
  public static void startSpark() {
    sparkSession = SparkSession.builder()
//      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.catalog.local.type", "hadoop")
//    .config("spark.sql.catalog.local.warehouse", "data/warehouse")
      // TODO[Saurabh]: Always tries to load HiveCatalog irrespective of what you put
//      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
//      .config("spark.sql.catalog.spark_catalog.warehouse", tableDir)
//      .enableHiveSupport()
      .master("local[2]") // spark://172.25.2.201:7077 to connect to remote cluster
      .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    sparkSession.stop();
  }

  @Before
  public void setUp() {
    table = new HadoopTables().load(tableDir);
  }
}
