package com.example.demo.service;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.demo.exceptions.DatasetNotFoundException;
import com.example.demo.model.UserAudit;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DemoService {

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

    BigQuery bigQuery;

    protected BigQuery getBigQueryService () throws IOException {
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

    public List<UserAudit> getUserAuditList (int page, int pageSize) throws DatasetNotFoundException, BigQueryException, InterruptedException, FileNotFoundException, IOException {
        try {                
            Dataset dataset = this.getBigQueryService().getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                //query to a existent bigquery table
                String query = "SELECT id\n"
                        + " FROM `"
                        + projectId
                        + "."
                        + datasetName
                        + "."
                        + tableName
                        + "`" 
                        + " LIMIT " + pageSize + " OFFSET " + (page * pageSize);
                log.info("query: {}", query);        
                return query(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException | IOException  e) {
            throw e;
        }                 
    }

    public void cleanUserAuditTable () throws BigQueryException, DatasetNotFoundException, InterruptedException, IOException {
        try {
            Dataset dataset = getBigQueryService().getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                //query to a existent bigquery table
                String query = "DELETE FROM `"
                        + projectId
                        + "."
                        + datasetName
                        + "."
                        + tableName
                        + "` WHERE 1=1"; 
                log.info("query: {}", query);        
                executeQuery(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException e) {
            throw e;
        }
    }

    public void insertIntoUserAuditTable () throws BigQueryException, DatasetNotFoundException, InterruptedException, IOException {
        try {
            Dataset dataset = getBigQueryService().getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                //query to a existent bigquery table
                // insert into `users_audit_eu_west3.users-audit` (id)
                //SELECT * FROM EXTERNAL_QUERY("swaggy-2-stage.europe-west3.swaggy_stage_mysql", "SELECT id FROM users_module.users_audit;");
                String query = "INSERT INTO `"
                        + projectId
                        + "."
                        + datasetName
                        + "."
                        + tableName
                        + "`" 
                        + " (id) "
                        + "SELECT * FROM EXTERNAL_QUERY('" + projectId + "." + region + "." + cloudSqlInstanceId + "', 'SELECT id FROM " + sqlDbName + "." + sqlTableName + "')";        
                log.info("query: {}", query);        
                executeQuery(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException | IOException e) {
            throw e;
        }
    }

    public List<UserAudit> getUserAuditListUsingExternalQuery (int page, int pageSize) throws DatasetNotFoundException, BigQueryException, InterruptedException, IOException {
        try {
            Dataset dataset = getBigQueryService().getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                // query to the cloud sql table, it create a temporary bigquery table         
                // non rispetta l'ordine del risultato della query esterna, anche se la tua query esterna include ORDER BY.
                String query = "SELECT * FROM EXTERNAL_QUERY('" + projectId + "." + region + "." + cloudSqlInstanceId + "', 'SELECT id FROM " + sqlDbName + "." + sqlTableName + " limit " + 
                pageSize + " offset " + (page * pageSize) +"')";
                log.info("query: {}", query);        
                return query(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException | IOException e) {
            throw e;
        }
                
    }
    
    protected List<UserAudit> query(String query) throws BigQueryException, InterruptedException, FileNotFoundException, IOException {
        var userAuditList = new ArrayList<UserAudit>();        
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            var jobId = JobInfo.newBuilder(queryConfig).build().getJobId();
            BigQuery bigQuery = getBigQueryService();
            TableResult results = bigQuery.query(queryConfig, jobId);
            if (results != null) {
                log.info("query performed with {} results", results.getTotalRows());
                results
                    .iterateAll()
                    .forEach(row -> {
                        var userAudit = new UserAudit();
                        userAudit.setId(row.get("id").getStringValue());
                        userAuditList.add(userAudit);
                    });
            }
        } catch (BigQueryException | InterruptedException | IOException e) {
            log.error("Query not performed \n" + e.toString());
            throw e;
        }

        return userAuditList;
    }

    private void executeQuery(String query) throws BigQueryException, InterruptedException, IOException {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = getBigQueryService().query(queryConfig);
            log.info("query performed with {} results", results.getTotalRows());
        } catch (BigQueryException | InterruptedException | IOException e) {
            log.error("Query not performed \n" + e.toString());
            throw e;
        }
    }
}
