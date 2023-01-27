package com.example.demo.controller;

import com.example.demo.model.UserAudit;
import com.example.demo.service.DemoService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DemoControllerTest {

    @InjectMocks
    DemoController demoController;

    @Mock
    DemoService demoService;

    @Test
    void getUsersAudit() throws Exception {
        var userAudit1 = new UserAudit();
        userAudit1.setId("1");
        var userAudit2 = new UserAudit();
        userAudit2.setId("2");
        when(demoService.getUserAuditList(anyInt(), anyInt())).thenReturn(List.of(userAudit1, userAudit2));
        var result = demoController.getUsersAudit(0,10);
        assertTrue(result.getStatusCode().is2xxSuccessful());
        var body = (List) result.getBody();
        assertEquals(body.size(), 2);
        var user1 = (UserAudit) body.get(0);
        var user2 = (UserAudit) body.get(1);
        assertEquals(user1.getId(), "1");
        assertEquals(user2.getId(), "2");
    }
}