package com.learn.shaik.destination;

import com.learn.shaik.model.ProductMessage;

public interface DestinationClient {

    void send(String productName, ProductMessage message);
}
