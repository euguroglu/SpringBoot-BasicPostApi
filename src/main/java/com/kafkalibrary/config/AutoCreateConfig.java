package com.kafkalibrary.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local") //This profile annonation allow us to use this configuration only during local development not prod
public class AutoCreateConfig {

    @Bean
    public NewTopic libraryEvents(){

       return TopicBuilder.name("event-library")
                .partitions(3)
                .replicas(3)
                .build();
    }
}
