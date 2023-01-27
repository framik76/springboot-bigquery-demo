package com.example.demo.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


import com.example.demo.utils.Toolbox;
import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.common.collect.ImmutableList;

@ExtendWith(MockitoExtension.class)
@SpringBootTest
@PropertySource("classpath:application.yml")
public class DemoServiceTest {

    @InjectMocks
    DemoService demoService;

    @Mock
    Toolbox toolbox;

    @Value("${gcp.bigquery.dataset.name}")
    String datasetName;

    @Value("${gcp.project.id}")
    String projectId;

    @Value("${gcp.bigquery.table.name}")
    String tableName;

    @BeforeEach
    void setUp() throws Exception {
        ReflectionTestUtils.setField(demoService, "datasetName", datasetName);
        ReflectionTestUtils.setField(demoService, "projectId", projectId);
        ReflectionTestUtils.setField(demoService, "tableName", tableName);
        lenient().when(toolbox.getDataset(any())).thenReturn(mock(Dataset.class));
        when(toolbox.getDataset()).thenReturn(mock(Dataset.class));
    }

    @Test
    void testGetUserAuditList() throws Exception {
        Page<FieldValueList> resulPage =
                new PageImpl<>(
                        new PageImpl.NextPageFetcher<FieldValueList>() {
                            @Override
                            public Page<FieldValueList> getNextPage() {
                                return null;
                            }
                        },
                        "abc",
                        ImmutableList.of(newFieldValueList("0"), newFieldValueList("1")));
        var tableResult = new TableResult(Schema.of(Field.of("id", LegacySQLTypeName.STRING)), 2, resulPage);

        when(toolbox.runQuery(anyString())).thenReturn(tableResult);

        var ret = demoService.getUserAuditList(0, 10);
        assertEquals(ret.size(), 2);
    }
 
    @Test
    void testQueryWithoutResults() throws Exception {
        Page<FieldValueList> resulPage =
            new PageImpl<>(
                new PageImpl.NextPageFetcher<FieldValueList>() {
                    @Override
                    public Page<FieldValueList> getNextPage() {
                    return null;
                    }
                },
                "abc",
                ImmutableList.of());

        var query = "select id from projectId.dataset.table";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        var tableResult = new TableResult(Schema.of(Field.of("id", LegacySQLTypeName.STRING)), 0, resulPage);
        when(toolbox.runQuery(anyString())).thenReturn(tableResult);
        var list = demoService.getUserAuditList(0, 10);
        assertEquals(list.size(), 0);
    }

    private static FieldValueList newFieldValueList(String s) {
        return FieldValueList.of(ImmutableList.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, s)));
    }

}
