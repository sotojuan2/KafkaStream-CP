package com.example.kafkastream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Hello world!
 *
 */
public class StreamStarterApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder =new StreamsBuilder();
        // 1 -Stream from kafka
        KStream<String,String> wordCountInput = builder.stream("word-count-input");
        // 2 - map values to lower case
        KTable<String,Long> wordCounts =  wordCountInput.mapValues(textLine ->textLine.toLowerCase())
            // 3 -flatmpa values split by spaces
            .flatMapValues(textLine -> Arrays.asList(textLine.split(" ")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((key, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count(Materialized.as("Counts"));
        
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        System.out.println("hola");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
