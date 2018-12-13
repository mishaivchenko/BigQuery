package persistence;

import helper.BigQueryHelperForTest;
import org.junit.*;
import persistence.impl.BigQueryRepositoryImpl;

import java.io.IOException;

public class BigQueryRepositoryTest {
    private static BigQueryHelperForTest bigQueryHelperForTest = new BigQueryHelperForTest("mytestproject-225210");
    private final String DATASET = "dataSet_for_test";

    @BeforeClass
    public static void before() {
        bigQueryHelperForTest.createDataSetForTest();
    }

    @AfterClass
    public static void after() {
        bigQueryHelperForTest.removeDataSet();
    }

    @After
    public void removeTables() {
        bigQueryHelperForTest.removeAllTables();
    }

    @Test
    public void addSingleTableTestAfterAddedDataSetShouldContainsTableWithExpectedName() throws IOException {
        //Given
        String expectedName = "name0";
        BigQueryRepositoryImpl bigQueryRepository = new BigQueryRepositoryImpl("mytestproject-225210", DATASET, "name");
        //When
        bigQueryRepository.addTables(1);
        String actual = bigQueryHelperForTest.getTable("name0").getTableReference().getTableId();
        //Then
        Assert.assertEquals(expectedName, actual);
    }

    @Test
    public void addFourTablesTestAfterAddedDataSetShouldContainsTablesWithExpectedNames() throws IOException {
        //Given
        String expectedName0 = "name0";
        String expectedName1 = "name1";
        String expectedName2 = "name2";
        String expectedName3 = "name3";
        BigQueryRepositoryImpl bigQueryRepository = new BigQueryRepositoryImpl("mytestproject-225210", DATASET, "name");
        //When
        bigQueryRepository.addTables(4);
        String actual0 = bigQueryHelperForTest.getTable("name0").getTableReference().getTableId();
        String actual1 = bigQueryHelperForTest.getTable("name1").getTableReference().getTableId();
        String actual2 = bigQueryHelperForTest.getTable("name2").getTableReference().getTableId();
        String actual3 = bigQueryHelperForTest.getTable("name3").getTableReference().getTableId();
        //Then
        Assert.assertEquals(expectedName0, actual0);
        Assert.assertEquals(expectedName1, actual1);
        Assert.assertEquals(expectedName2, actual2);
        Assert.assertEquals(expectedName3, actual3);
    }

    @Test
    public void fieldsTestTableMustContainedFieldsWithExpectedNames() throws IOException {
        //Given
        String expectedField_1 = "hitId";
        String expectedField_2 = "userId";
        BigQueryRepositoryImpl bigQueryRepository = new BigQueryRepositoryImpl("mytestproject-225210", DATASET, "name");
        //When
        bigQueryRepository.addTables(1);
        String actualName_1 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(0).getName();
        String actualName_2 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(1).getName();
        //Then
        Assert.assertEquals(expectedField_1,actualName_1);
        Assert.assertEquals(expectedField_2,actualName_2);
    }

    @Test
    public void fieldsTestTableMustContainedFieldsWithExpectedModes() throws IOException {
        //Given
        String expectedField_1 = "NULLABLE";
        String expectedField_2 = "NULLABLE";
        BigQueryRepositoryImpl bigQueryRepository = new BigQueryRepositoryImpl("mytestproject-225210", DATASET, "name");
        //When
        bigQueryRepository.addTables(1);
        String actualMode_1 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(0).getMode();
        String actualMode_2 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(1).getMode();
        //Then
        Assert.assertEquals(expectedField_1,actualMode_1);
        Assert.assertEquals(expectedField_2,actualMode_2);
    }
    @Test
    public void fieldsTestTableMustContainedFieldsWithExpectedTypes() throws IOException {
        //Given
        String expectedField_1 = "STRING";
        String expectedField_2 = "STRING";
        BigQueryRepositoryImpl bigQueryRepository = new BigQueryRepositoryImpl("mytestproject-225210", DATASET, "name");
        //When
        bigQueryRepository.addTables(1);
        String actualType_1 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(0).getType();
        String actualType_2 = bigQueryHelperForTest.getTable("name0").getSchema().getFields().get(1).getType();
        //Then
        Assert.assertEquals(expectedField_1,actualType_1);
        Assert.assertEquals(expectedField_2,actualType_2);
    }
}
