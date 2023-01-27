package com.example.demo.utils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

@Slf4j
@Component
public class Toolbox {

    @Value("${gcp.project.id}")
    String projectId;

    @Value("${gcp.bigquery.dataset.name}")
    String datasetName;

    @Value("${gcp.bigquery.table.name}")
    String tableName;

    @Value("${gcp.sql.region}")
    String region;

    @Value("${gcp.sql.instance.id}")
    String cloudSqlInstanceId;

    @Value("${gcp.sql.db.name}")
    String sqlDbName;

    @Value("${gcp.sql.table.name}")
    String sqlTableName;

    @Value("${gcp.credentials.encoded-key}")
    String encodedKey;

    private static BigQuery bigQuery;

    private static Toolbox _instance = new Toolbox();


    private BigQuery getBigQueryService () throws IOException {
        try {
            var credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(encodedKey)));
            bigQuery = BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials)
                    .build().getService();
        } catch (IOException e) {
            throw e;
        }
        return bigQuery;
    }

    public Dataset getDataset() throws IOException {
        Dataset dataset = getBigQueryService().getDataset(DatasetId.of(datasetName));
        return dataset;
    }

    public Dataset getDataset(DatasetId datasetId) throws IOException {
        Dataset dataset = getBigQueryService().getDataset(datasetId);
        return dataset;
    }

    /**
     * The method runBqQuery is a generic method to call any BigQuery Sql. It will return the result
     * of BigQuery Sql as an object of TableResult.
     *
     * @param query : sql query to execute in bigquery
     * @return
     * @throws InterruptedException
     */
    public TableResult runQuery(String query) throws InterruptedException, IOException {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = getBigQueryService().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            TableResult result = queryJob.getQueryResults();
            log.info("query performed with {} results", result.getTotalRows());
            return result;
        } catch (InterruptedException | IOException e) {
            log.error("Query not performed: {}", e.getMessage());
            throw e;
        }
    }

}
