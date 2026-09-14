package com.learn.shaik.jcsmp;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "solace.jcsmp")
public class JcsmpProperties {

    private String host;
    private String vpn;
    private String username;
    private String password;
    private String acknowledgementMode;
    private int messageWindowSize;
}
