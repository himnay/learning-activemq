package com.learnactivemq.publisher;

import com.learnactivemq.common.config.JmsEventConverterConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(JmsEventConverterConfig.class)
class PublisherApplication {

    static void main(String[] args) {
        SpringApplication.run(PublisherApplication.class, args);
    }
}
