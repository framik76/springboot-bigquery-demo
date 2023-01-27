package com.example.demo.service;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.example.demo.utils.Toolbox;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.demo.exceptions.DatasetNotFoundException;
import com.example.demo.model.UserAudit;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
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

    @Autowired
    Toolbox toolbox;

    /**
     * The method getUserAuditList make a query to an existent bigQuery table.
     *
     * @param page : a integer indicating the page (start from 0)
     * @param pageSize : a integer indicating the items returned
     * @return List<UserAudit>
     * @throws DatasetNotFoundException
     * @throws BigQueryException
     * @throws InterruptedException
     * @throws IOException
     */
    public List<UserAudit> getUserAuditList (int page, int pageSize) throws DatasetNotFoundException, BigQueryException, InterruptedException, IOException {
        try {
            Dataset dataset = toolbox.getDataset();
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
                return executeQueryAndReturnResult(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException | IOException  e) {
            throw e;
        }                 
    }


    /**
     * The method cleanUserAuditTable make a query to delete all records an existent bigQuery table.
     *
     * @throws DatasetNotFoundException
     * @throws BigQueryException
     * @throws InterruptedException
     * @throws IOException
     */
    public void cleanUserAuditTable () throws BigQueryException, DatasetNotFoundException, InterruptedException, IOException {
        try {
            Dataset dataset = toolbox.getDataset(DatasetId.of(datasetName));
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

    /**
     * The method insertIntoUserAuditTable make a query to insert all records of a cloud sql table into an existent bigQuery table.
     *
     * @throws DatasetNotFoundException
     * @throws BigQueryException
     * @throws InterruptedException
     * @throws IOException
     */
    public void insertIntoUserAuditTable () throws BigQueryException, DatasetNotFoundException, InterruptedException, IOException {
        try {
            Dataset dataset = toolbox.getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                //insert query from a cloud sql table into a existent bigquery table
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

    /**
     * The method getUserAuditListUsingExternalQuery make a query to a cloud sql table returning a temporary bigQuery table.
     *
     * @param page : a integer indicating the page (start from 0)
     * @param pageSize : a integer indicating the items returned
     * @return List<UserAudit>
     * @throws DatasetNotFoundException
     * @throws BigQueryException
     * @throws InterruptedException
     * @throws IOException
     */
    public List<UserAudit> getUserAuditListUsingExternalQuery (int page, int pageSize) throws DatasetNotFoundException, BigQueryException, InterruptedException, IOException {
        try {
            Dataset dataset = toolbox.getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                // query to the cloud sql table, it creates a temporary bigquery table
                // the order of the result of the external query is not respected even if the external query include ORDER BY clause.
                String query = "SELECT * FROM EXTERNAL_QUERY('" + projectId + "." + region + "." + cloudSqlInstanceId + "', 'SELECT id FROM " + sqlDbName + "." + sqlTableName + " limit " + 
                pageSize + " offset " + (page * pageSize) +"')";
                log.info("query: {}", query);        
                return executeQueryAndReturnResult(query);
            } else {
                throw new DatasetNotFoundException("Dataset not found.");
            }
        } catch (BigQueryException | IOException e) {
            throw e;
        }
                
    }
    
    protected List<UserAudit> executeQueryAndReturnResult(String query) throws BigQueryException, InterruptedException, IOException {
        var userAuditList = new ArrayList<UserAudit>();
        try {
            TableResult results = toolbox.runQuery(query);
            if (results != null) {
                results
                    .iterateAll()
                    .forEach(row -> {
                        var userAudit = new UserAudit();
                        userAudit.setId(row.get("id").getStringValue());
                        userAuditList.add(userAudit);
                    });
            }
        } catch (BigQueryException | InterruptedException | IOException e) {
            throw e;
        }

        return userAuditList;
    }

    private void executeQuery(String query) throws BigQueryException, InterruptedException, IOException {
        try {
            toolbox.runQuery(query);
        } catch (BigQueryException | InterruptedException | IOException e) {
            throw e;
        }
    }
}
