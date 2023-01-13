package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.exceptions.DatasetNotFoundException;
import com.example.demo.service.DemoService;
import com.google.cloud.bigquery.BigQueryException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/")
@AllArgsConstructor
@Slf4j
public class DemoController {

    @Autowired
    DemoService demoService;

    @GetMapping("/users-audit")
    public ResponseEntity<?> getUsersAudit(@RequestParam("page") int page, @RequestParam("pageSize") int pageSize) {
        try {
            return ResponseEntity.ok(demoService.getUserAuditList(page, pageSize));
        } catch (DatasetNotFoundException e) {
            log.error("DatasetNotFoundException: ", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (BigQueryException e) {
            log.error("BigQueryException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        } catch (InterruptedException e) {
            log.error("InterruptedException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        }        
    }

    @DeleteMapping("/users-audit")
    public ResponseEntity<?> cleanUsersAudit() {
        try {
            demoService.cleanUserAuditTable();
            return ResponseEntity.ok(true);
        } catch (DatasetNotFoundException e) {
            log.error("DatasetNotFoundException: ", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (BigQueryException e) {
            log.error("BigQueryException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        } catch (InterruptedException e) {
            log.error("InterruptedException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        }        
    }

    @PutMapping("/users-audit")
    public ResponseEntity<?> insertUsersAudit() {
        try {
            demoService.insertIntoUserAuditTable();
            return ResponseEntity.ok(true);
        } catch (DatasetNotFoundException e) {
            log.error("DatasetNotFoundException: ", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (BigQueryException e) {
            log.error("BigQueryException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        } catch (InterruptedException e) {
            log.error("InterruptedException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        }        
    }
    
    @GetMapping("/users-audit-external")
    public ResponseEntity<?> getExternalUsersAudit(@RequestParam("page") int page, @RequestParam("pageSize") int pageSize) {
        try {
            return ResponseEntity.ok(demoService.getUserAuditListUsingExternalQuery(page, pageSize));
        } catch (DatasetNotFoundException e) {
            log.error("DatasetNotFoundException: ", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (BigQueryException e) {
            log.error("BigQueryException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        } catch (InterruptedException e) {
            log.error("InterruptedException: ", e.getMessage());
            return ResponseEntity.internalServerError().body(e.getMessage());
        }        
    }

}
