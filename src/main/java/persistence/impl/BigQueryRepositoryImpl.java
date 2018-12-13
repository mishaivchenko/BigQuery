package persistence.impl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;
import persistence.Repository;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BigQueryRepositoryImpl implements Repository {

    private final String PROJECT_ID;
    private final String DATASET_ID;
    private final String PREFIX;

    private Bigquery bigquery = createAuthorizedClient();

    public BigQueryRepositoryImpl(String projectId, String datasetId, String prefix) {
        PROJECT_ID = projectId;
        DATASET_ID = datasetId;
        PREFIX = prefix;
    }

    public void addTables(int count) throws IOException {
        int number = getNumberOfLastTable();
        for (int i = number; i < number + count; i++) {
            createTable(i);
        }
    }

    private void createTable(int number) throws IOException {
        List<TableFieldSchema> fieldSchema = new ArrayList<>();
        fieldSchema.add(new TableFieldSchema().setName("hitId").setType("STRING").setMode("NULLABLE"));
        fieldSchema.add(new TableFieldSchema().setName("userId").setType("STRING").setMode("NULLABLE"));
        TableSchema schema = new TableSchema();
        schema.setFields(fieldSchema);
        TableReference ref = new TableReference();
        ref.setProjectId(PROJECT_ID);
        ref.setDatasetId(DATASET_ID);
        ref.setTableId(PREFIX + number);
        Table content = new Table();
        content.setTableReference(ref);
        content.setSchema(schema);
        bigquery.tables().insert(ref.getProjectId(), ref.getDatasetId(), content).execute();
    }

    private int getNumberOfLastTable() throws IOException {
        List<TableRow> rows =
                executeQuery(
                        bigquery,
                        PROJECT_ID);
        if (rows == null) {
            return 0;
        }
        return rows.size() + 1;
    }

    private List<TableRow> executeQuery(Bigquery bigquery, String projectId)
            throws IOException {
        QueryResponse query =
                bigquery.jobs().query(projectId, new QueryRequest().setQuery("SELECT * FROM " + DATASET_ID + ".__TABLES_SUMMARY__")).execute();
        GetQueryResultsResponse queryResult =
                bigquery
                        .jobs()
                        .getQueryResults(
                                query.getJobReference().getProjectId(), query.getJobReference().getJobId())
                        .execute();
        return queryResult.getRows();
    }

    private Bigquery createAuthorizedClient() {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = null;
        try {
            credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        } catch (IOException e) {
            try {
                credential = GoogleCredential.fromStream(
                        new FileInputStream("src/APPLICATION_CREDENTIALS.json"),
                        transport,
                        jsonFactory);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        if (Objects.requireNonNull(credential).createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Bigquery Samples")
                .build();
    }


}
