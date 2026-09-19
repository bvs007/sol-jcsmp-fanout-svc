package com.learn.shaik.destination;

import com.learn.shaik.model.ProductMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestClient;

@Slf4j
public class HttpDestinationClient implements DestinationClient {

    private final RestClient restClient;
    private final String baseUrl;

    public HttpDestinationClient(String baseUrl) {

        this.baseUrl = baseUrl;

        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public void send(
            String productName,
            ProductMessage message) {

        log.info(
                "Calling downstream. product={} requestId={} url={}",
                productName,
                message.getRequestId(),
                baseUrl + "/test/downstream/" + productName
        );

        String response =
                restClient
                        .post()
                        .uri(
                                "/test/downstream/{productName}",
                                productName
                        )
                        .body(message)
                        .retrieve()
                        .body(String.class);

        log.info(
                "Downstream completed. product={} requestId={} response={}",
                productName,
                message.getRequestId(),
                response
        );
    }
}