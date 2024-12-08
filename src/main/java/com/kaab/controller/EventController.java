package com.kaab.controller;

import com.kaab.dto.Customer;
import com.kaab.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            for (int i=0; i<100000; i++) {
                kafkaMessagePublisher.sendMessagetoTopic(message + " : " + i);
            }

            return ResponseEntity.ok("Message published");
        }catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish")
    public void sendEvent(@RequestBody Customer customer) {
        kafkaMessagePublisher.sendEvenetToTopic(customer);
    }
}
