/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * TestHiveGlueMetastoreUsingThrift requires AWS credentials to be correctly set up in order to access Glue catalog.
 * You can configure credentials locally in ~/.aws/credentials before running the test. Alternatively, you can set up
 * AWS access/secret keys or IAM role in `setUpGlueCatalog` method in the test suite using the Glue config properties
 * from https://prestodb.io/docs/current/connector/hive.html#aws-glue-catalog-configuration-properties.
 */
abstract class TestHiveQueriesGlueMetastore
        extends TestHiveQueries
{
    private static final String TEMPORARY_PREFIX = "tmp_presto_glue_test_";
    // Catalog name used in the tests, it is also hardcoded in HiveExternalWorkerQueryRunner so workers can register
    // the connector so if you want to change the name, also update it in HiveExternalWorkerQueryRunner.
    private static final String CATALOG_NAME = "test_glue";

    // Warehouse directory for the Glue catalog.
    private File tempWarehouseDir;
    // Fully-qualified schema name in format [catalog].[schema] used in tests.
    private String schemaName;

    private final boolean useThrift;

    protected TestHiveQueriesGlueMetastore(boolean useThrift)
    {
        super(useThrift);
        this.useThrift = useThrift;
    }
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String dataDirectory = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath, "Native worker binary path is missing. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.");
        assertNotNull(dataDirectory, "Data directory path is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments.");

        return HiveExternalWorkerQueryRunner.createNativeQueryRunner(
                dataDirectory,
                prestoServerPath,
                Optional.ofNullable(workerCount).map(Integer::parseInt),
                cacheMaxSize,
                useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.of(Paths.get(dataDirectory)));
    }

    private static void setUpGlueCatalog(QueryRunner queryRunner, File tempWarehouseDir) throws IOException
    {
        assertNotNull(queryRunner, "Query runner must be provided");
        assertNotNull(tempWarehouseDir, "Temp warehouse directory must be provided");

        queryRunner.installPlugin(new HivePlugin("glue"));
        Map<String, String> glueProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "glue")
                .put("hive.metastore.glue.region", "us-east-1")
                .put("hive.metastore.glue.default-warehouse-dir", tempWarehouseDir.getCanonicalPath())
                // This option is required to avoid the error:
                // "Table scan with filter pushdown disabled is not supported".
                // Velox requires this flag to be set for scans.
                .put("hive.pushdown-filter-enabled", "true")
                // The options below are required to avoid "Access Denied" errors when performing operations such
                // as DROP TABLE.
                .put("hive.max-partitions-per-writers", "999")
                .put("hive.allow-drop-table", "true")
                .put("hive.allow-rename-table", "true")
                .put("hive.allow-rename-column", "true")
                .put("hive.allow-add-column", "true")
                .put("hive.allow-drop-column", "true")
                .build();

        queryRunner.createCatalog("test_glue", "glue", glueProperties);
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                // This option is required to avoid the error:
                // "Unknown plan node type com.facebook.presto.sql.planner.plan.TableWriterMergeNode".
                // TableWriterMergeNode is not supported by Velox yet.
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .build();
    }

    private static String getRandomName(String prefix)
    {
        String randomName = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return TEMPORARY_PREFIX + prefix + randomName;
    }

    /**
     * Returns the fully-qualified schema name.
     */
    private static String tempSchema(String prefix)
    {
        return CATALOG_NAME + "." + getRandomName(prefix);
    }

    /**
     * Returns the fully-qualified table name.
     */
    private static String tempTable(String schemaName, String prefix)
    {
        return schemaName + "." + getRandomName(prefix);
    }

    /**
     * Asserts that column names in the `tableName` match the expected column names.
     */
    private void assertColumns(String tableName, Set<String> expectedColumnNames)
    {
        MaterializedResult res = getQueryRunner().execute("show columns in " + tableName);
        Set<String> columnNames = new HashSet<String>();
        for (MaterializedRow row : res.getMaterializedRows()) {
            columnNames.add(row.getField(0).toString());
        }
        assertEquals(columnNames, expectedColumnNames);
    }

    @BeforeClass
    public void setUp() throws IOException
    {
        tempWarehouseDir = Files.createTempDir();
        setUpGlueCatalog(getQueryRunner(), tempWarehouseDir);
        setUpGlueCatalog((QueryRunner) getExpectedQueryRunner(), tempWarehouseDir);

        schemaName = tempSchema("test_schema");
        getQueryRunner().execute("create schema " + schemaName);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException
    {
        deleteRecursively(tempWarehouseDir.toPath(), ALLOW_INSECURE);
        if (schemaName != null) {
            getQueryRunner().execute("drop schema if exists " + schemaName);
        }
    }

    @Test
    public void testShowCatalogs()
    {
        // Glue catalog should be configured correctly.
        MaterializedResult res = getQueryRunner().execute("show catalogs like '%" + CATALOG_NAME + "%'");
        assertEquals(res.getMaterializedRows().size(), 1);
        for (MaterializedRow row : res.getMaterializedRows()) {
            assertEquals(row.getField(0), CATALOG_NAME);
        }
    }

    @Test
    public void testCreateDropSchema() throws Exception
    {
        String schemaName = tempSchema("default");
        getQueryRunner().execute("create schema " + schemaName);
        getQueryRunner().execute("drop schema " + schemaName);
    }

    @Test
    public void testCreateInsertDropTable() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "create table " + tableName + "(a int, b int) WITH (format = 'DWRF')");
            // TODO: Figure out why velox fails inserting into a table.
            getQueryRunner().execute(session, "insert into " + tableName + " values (1, 2)");
            assertQuery(session, "select * from " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "drop table if exists " + tableName);
        }
    }

    @Test
    public void testCreateView() throws Exception
    {
        String viewName = tempTable(schemaName, "test_view");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "create view " + viewName + " as select * from customer");
            assertQuery(session, "select * from " + viewName);
        }
        finally {
            getQueryRunner().execute(session, "drop view if exists " + viewName);
        }
    }

    @Test
    public void testRenameTable() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        String newTableName = tempTable(schemaName, "renamed_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "create table " + tableName + " as select * from customer");
            assertQueryFails(session,
                    "alter table " + tableName + " rename to " + newTableName,
                    "Table rename is not yet supported by Glue service");
        }
        finally {
            getQueryRunner().execute("drop table if exists " + tableName);
        }
    }

    @Test
    public void testAddRenameDropColumn() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            // Add column.
            getQueryRunner().execute(session, "create table " + tableName + " WITH (format = 'DWRF') as select acctbal, custkey from customer");
            getQueryRunner().execute(session, "alter table " + tableName + " add column tmp_col INT COMMENT 'test column'");
            assertQuery(session, "select * from " + tableName);
            assertColumns(tableName, ImmutableSet.of("acctbal", "custkey", "tmp_col"));

            // Rename column.
            getQueryRunner().execute(session, "alter table " + tableName + " rename column tmp_col to tmp_col2");
            assertQuery(session, "select * from " + tableName);
            assertColumns(tableName, ImmutableSet.of("acctbal", "custkey", "tmp_col2"));

            // Drop column.
            getQueryRunner().execute(session, "alter table " + tableName + " drop column tmp_col2");
            assertQuery(session, "select * from " + tableName);
            assertColumns(tableName, ImmutableSet.of("acctbal", "custkey"));
        }
        finally {
            getQueryRunner().execute(session, "drop table if exists " + tableName);
        }
    }

    @Test
    public void testCreateInsertDropPartitionedTable() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "CREATE TABLE " + tableName + " (name VARCHAR, address VARCHAR, mktsegment VARCHAR) WITH (format = 'DWRF', partitioned_by = ARRAY['mktsegment'])");
            getQueryRunner().execute(session, "INSERT INTO " + tableName + " SELECT name, address, mktsegment FROM customer");
            assertQuery(session, "select * from " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "drop table if exists " + tableName);
        }
    }

    @Test
    public void testPartitionFilters() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session,
                    "CREATE TABLE " + tableName + "(" +
                            "orderkey bigint, " +
                            "custkey bigint, " +
                            "orderstatus varchar(1), " +
                            "totalprice double, " +
                            "orderdate varchar, " +
                            "shippriority int" +
                            ") WITH (format = 'DWRF', partitioned_by = ARRAY['orderdate', 'shippriority'])");
            getQueryRunner().execute(session,
                    "INSERT INTO " + tableName + " SELECT " +
                            "orderkey, custkey, orderstatus, totalprice, orderdate, shippriority FROM orders LIMIT 10");

            assertQuery(session, "select * from " + tableName);
            assertQuery(session, "select * from " + tableName + " where orderdate = '1996-04-02'");
            assertQuery(session, "select * from " + tableName + " where orderdate = '1996-04-02' and shippriority = 0");
            assertQuery(session, "select * from " + tableName + " where orderdate = '1996-04-02' or orderdate = '1996-06-26'");

            assertQuery(session, "select * from " + tableName + " where shippriority = 1");
            assertQuery(session, "select * from " + tableName + " where orderdate = '2023-03-01' or orderdate = '1990-01-01'");
        }
        finally {
            getQueryRunner().execute(session, "drop table if exists " + tableName);
        }
    }

    @Test
    public void testAnalyzeTable() throws Exception
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session,
                    "CREATE TABLE " + tableName + " WITH (format = 'DWRF', partitioned_by = ARRAY['orderdate', 'shippriority']) " +
                    "AS SELECT orderkey, custkey, orderstatus, totalprice, orderdate, shippriority FROM orders LIMIT 10");
            getQueryRunner().execute(session, "ANALYZE " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "drop table if exists " + tableName);
        }
    }

    @Test
    @Override
    public void testCatalogWithCacheEnabled()
    {
        // We don't need to run this test.
    }
}
