package com.learn.shaik.test.controller;

import com.learn.shaik.model.ProductMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/test/downstream")
public class DownstreamTestController {

    @PostMapping("/{productName}")
    public ResponseEntity<String> receive(
            @PathVariable String productName,
            @RequestBody ProductMessage message) {

        log.info(
                "DOWNSTREAM RECEIVED | product={} requestId={} requestType={} payload={}",
                productName,
                message.getRequestId(),
                message.getRequestType(),
                message.getPayload()
        );

        return ResponseEntity.ok(
                "Processed successfully"
        );
    }
}