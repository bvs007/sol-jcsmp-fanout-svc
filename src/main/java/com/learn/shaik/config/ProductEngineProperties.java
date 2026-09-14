package com.learn.shaik.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "product-engine")
public class ProductEngineProperties {

    private Map<String, ProductConfig> products;
}
