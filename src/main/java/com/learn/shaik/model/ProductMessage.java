package com.learn.shaik.model;

import lombok.Data;

@Data
public class ProductMessage {

    private String requestId;
    private String requestType;
    private String payload;
}
