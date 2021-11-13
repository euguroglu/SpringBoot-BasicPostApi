package com.kafkalibrary.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalibrary.domain.LibraryEvent;
import com.kafkalibrary.domain.LibraryEventType;
import com.kafkalibrary.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    //Create api post end-point to receive post message
    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

    //Invoke kafka producer
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        log.info("before-sendLibraryEvent"); // To observe asyncronus behaviour
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        log.info("after-sendLibraryEvent");
        //libraryEventProducer.sendLibraryEventSynchronous(libraryEvent) 
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    //PUT
}
