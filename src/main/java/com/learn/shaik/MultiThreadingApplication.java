package com.learn.shaik;

import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.jcsmp.JcsmpProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
        ProductEngineProperties.class,
        JcsmpProperties.class
})
public class MultiThreadingApplication {

    public static void main(String[] args) {
        SpringApplication.run(
                MultiThreadingApplication.class,
                args
        );
    }
}
