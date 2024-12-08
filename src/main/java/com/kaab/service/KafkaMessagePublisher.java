package com.kaab.service;

import com.kaab.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessagetoTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("kaab-topic-2", message);
        future.whenComplete((result, ex) -> {
            if(ex == null){
                System.out.println("Send message =[ "+message+" ] with offset = ["+
                        result.getRecordMetadata().offset()+"]");
            }else {
                System.out.println("Unable to send message [" + message + "]" + "due to " + ex.getMessage());
            }
        });
    }


    public void sendEvenetToTopic(Customer customer){
        try {

            CompletableFuture<SendResult<String, Object>> future = template.send("kaab-topic-class", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Send Customer =[ " + customer + " ] with offset = [" +
                            result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message [" + customer + "]" + "due to " + ex.getMessage());
                }
            });

        }catch (Exception e){
            System.out.println("Unable to send message [" + customer + "]" + "due to " + e.getMessage());
        }
    }
}
