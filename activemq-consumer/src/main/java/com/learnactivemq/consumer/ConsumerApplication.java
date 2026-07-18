package com.learnactivemq.consumer;

import com.learnactivemq.common.config.JmsEventConverterConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;

@EnableJms
@SpringBootApplication
@Import(JmsEventConverterConfig.class)
class ConsumerApplication {

    /** Application entry point. */
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
