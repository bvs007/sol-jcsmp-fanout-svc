package com.learn.shaik.test.controller;

import com.learn.shaik.test.jcsmp.publisher.JcsmpTestPublisher;
import com.learn.shaik.test.model.TestPublishRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/test/publish")
@RequiredArgsConstructor
public class TestPublishController {

    private final JcsmpTestPublisher publisher;

    @PostMapping("/{productName}")
    public ResponseEntity<?> publish(
            @PathVariable String productName,
            @RequestBody(required = false)
            TestPublishRequest request) {

        if (request == null) {
            request = new TestPublishRequest();
        }

        try {

            List<String> requestIds =
                    publisher.publish(productName, request);

            return ResponseEntity.ok(
                    Map.of(
                            "product", productName,
                            "count", requestIds.size(),
                            "requestIds", requestIds
                    )
            );

        } catch (IllegalArgumentException e) {

            return ResponseEntity.badRequest().body(
                    Map.of("error", e.getMessage())
            );

        } catch (Exception e) {

            return ResponseEntity.internalServerError().body(
                    Map.of("error", e.getMessage())
            );
        }
    }
}