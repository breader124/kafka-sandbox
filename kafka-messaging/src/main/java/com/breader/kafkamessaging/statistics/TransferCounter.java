package com.breader.kafkamessaging.statistics;

import com.breader.kafkamessaging.model.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("transfers")
public class TransferCounter {
    private static final Logger logger = LoggerFactory.getLogger(TransferCounter.class);

    private Transfer lastTransfer;

    @KafkaListener(topics = "standard-transfers", groupId = "transfer", containerFactory = "transferContainerFactory")
    public void transferListener(@Payload Transfer t) {
        logger.info("to: {}, from: {}, amount: {}", t.getTo(), t.getFrom(), t.getAmount());
        lastTransfer = t;
    }

    @GetMapping("last")
    public Transfer getLastTransfer() {
        return lastTransfer;
    }
}
