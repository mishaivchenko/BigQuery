package helper;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

public class BigQueryHelperForTest {
    private final String PROJECT_ID;
    private final String DATASET_ID = "dataSet_for_test";
    private Bigquery bigquery = createBigQuery();

    public BigQueryHelperForTest(String projectId) {
        PROJECT_ID = projectId;
    }

    public void createDataSetForTest() {
        Dataset dataset = new Dataset();
        DatasetReference datasetReference = new DatasetReference();
        datasetReference
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET_ID);
        dataset.setDatasetReference(datasetReference);
        try {
            bigquery.datasets().insert(PROJECT_ID, dataset).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Table getTable(String tableId) throws IOException {
        return bigquery.tables().get(PROJECT_ID, DATASET_ID, tableId).execute();
    }

    public void removeAllTables() {
        try {
            for (TableList.Tables t : bigquery.tables().list(PROJECT_ID, DATASET_ID).execute().getTables()) {
                bigquery.tables().delete(PROJECT_ID, DATASET_ID, t.getTableReference().getTableId()).execute();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void removeDataSet() {
        try {
            bigquery.datasets().delete(PROJECT_ID, DATASET_ID).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Bigquery createBigQuery() {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = null;
        try {
            credential = GoogleCredential.fromStream(
                    new FileInputStream("src/test/APPLICATION_CREDENTIALS_TEST.json"),
                    transport,
                    jsonFactory);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        if (Objects.requireNonNull(credential).createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("FOR_TEST_ONLY")
                .build();
    }
}
