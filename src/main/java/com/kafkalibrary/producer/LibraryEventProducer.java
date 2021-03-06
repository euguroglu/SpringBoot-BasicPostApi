package com.kafkalibrary.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalibrary.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    String topic = "event-library";

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException{

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer,String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSucces(key, value, result);
            }
        });


    }
    public void sendLibraryEvent2(LibraryEvent libraryEvent) throws JsonProcessingException{

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        //Producer record can be used instead of (topic, key, value) as input in the kafkaTemplate.send method
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        ListenableFuture<SendResult<Integer,String>> listenableFuture = kafkaTemplate.send("event-library", key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSucces(key, value, result);
            }
        });


    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes(StandardCharsets.UTF_8)));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer,String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.info("ExecutionException, InterruptedException sending the message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.info("Exception sending the message and the exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;

    }
    private void handleSucces(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Succesfully for the key: {} and the value is {}, partition is {}", key, value, result.getRecordMetadata().partition());
    }
    private void handleFailure(Integer key, String value, Throwable ex){
        log.error("Error Sending the Message and the excepton is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in onFailure: {}", throwable.getMessage());
        }

    }
}

