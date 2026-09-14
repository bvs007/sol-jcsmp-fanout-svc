package com.learn.shaik.jcsmp;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class JcsmpSessionManager {

    private final JcsmpProperties properties;

    private JCSMPSession session;

    @PostConstruct
    public void connect() throws JCSMPException {

        JCSMPProperties jcsmpProperties =
                new JCSMPProperties();

        jcsmpProperties.setProperty(
                JCSMPProperties.HOST,
                properties.getHost()
        );

        jcsmpProperties.setProperty(
                JCSMPProperties.VPN_NAME,
                properties.getVpn()
        );

        jcsmpProperties.setProperty(
                JCSMPProperties.USERNAME,
                properties.getUsername()
        );

        jcsmpProperties.setProperty(
                JCSMPProperties.PASSWORD,
                properties.getPassword()
        );

        jcsmpProperties.setProperty(
                JCSMPProperties.MESSAGE_ACK_MODE,
                JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT
        );

        jcsmpProperties.setProperty(
                JCSMPProperties.SUB_ACK_WINDOW_SIZE,
                properties.getMessageWindowSize()
        );

        session =
                JCSMPFactory.onlyInstance()
                        .createSession(jcsmpProperties);

        session.connect();

        log.info(
                "JCSMP session connected. host={} vpn={}",
                properties.getHost(),
                properties.getVpn()
        );
    }

    public JCSMPSession getSession() {
        return session;
    }

    @PreDestroy
    public void disconnect() {

        if (session != null) {
            try {
                session.closeSession();
                log.info("JCSMP session closed");
            } catch (Exception e) {
                log.error(
                        "Error closing JCSMP session",
                        e
                );
            }
        }
    }
}
