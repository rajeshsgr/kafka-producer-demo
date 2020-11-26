package com.raj.nola.kafka.demo.config;

public class SystemConfig {

    public final static String applicationID = "ProducerDemoApp";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String topicName = "time-series";
    public final static int maxEventCount = 1000000;

}

