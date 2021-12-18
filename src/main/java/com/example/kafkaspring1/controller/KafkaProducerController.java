package com.example.kafkaspring1.controller;

import com.example.kafkaspring1.RandoMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.*;


import java.util.concurrent.ThreadLocalRandom;
@Slf4j
@RestController
@RequestMapping(value = "/kafka")
public class KafkaProducerController
{


    List<Map<String, String>> messages = new ArrayList<Map<String, String>>();

    @Autowired
    private KafkaTemplate<String,Object> temp;


    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestBody RandoMessage message)
    {
        temp.send("test",message);

    }


    @GetMapping(value = "/lastMessage")
    public Map<String, String> consumeMsg() {
        log.info(String.valueOf(messages));
       log.info(String.valueOf(messages.get(messages.size()-1)));
        return messages.get(messages.size()-1);
    }


    @KafkaListener(groupId = "group_id", topics = "test", containerFactory = "kafkaListenerContainerFactory")
    public List<Map<String, String>> getMsgFromTopic(RandoMessage data) {
        for (int i=0; i< data.getRandoMessage().size(); i++) {
            String field = String.valueOf(data.getRandoMessage().get(i).keySet()).substring(1,7);
            String type = data.getRandoMessage().get(i).get(field);
            if ( type.equals("number"))
            {
                Random rand = new Random();
                int number = rand.nextInt(10000);
                data.getFieldMessage().put(field, String.valueOf(number));
            }
            else if (type.equals("string"))
            {
               type= givenUsingPlainJava_whenGeneratingRandomStringBounded_thenCorrect();
                data.getFieldMessage().put(field, type);
            }
        }
        messages.add(data.getFieldMessage());
        log.info(String.valueOf(messages));
        return messages;
    }

    public String givenUsingPlainJava_whenGeneratingRandomStringBounded_thenCorrect() {

        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();

       return generatedString;
    }
}