package com.learn.shaik.test.model;

import lombok.Data;

@Data
public class TestPublishRequest {

    private int count = 1;
    private String requestType = "TEST";
    private String payload = "Hello from JCSMP test publisher";
}