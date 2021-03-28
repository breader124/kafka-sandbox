package com.breader.kafkamessaging.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("statistics")
public class MessagesCounter {
    private static final Logger logger = LoggerFactory.getLogger(MessagesCounter.class);

    private long messagesCounter = 0;

    @KafkaListener(topics = "spring-boot-messaging", groupId = "foo", containerFactory = "containerFactory")
    public void messagesListener(@Payload String value, @Header(KafkaHeaders.OFFSET) int offset) {
        ++messagesCounter;
        logger.info("value: {}, offset: {}", value, offset);
    }

    @GetMapping("messages-counter")
    public long howManyMessages() {
        return messagesCounter;
    }

}
