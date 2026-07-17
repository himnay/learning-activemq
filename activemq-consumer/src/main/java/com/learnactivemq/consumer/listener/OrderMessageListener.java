package com.learnactivemq.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderMessageListener {

    @JmsListener(destination = "${app.queue}")
    public void onMessage(String content, @Header(name = "messageId", required = false) String messageId) {
        log.info("Consumed message id={} payload={}", messageId, content);
    }
}
