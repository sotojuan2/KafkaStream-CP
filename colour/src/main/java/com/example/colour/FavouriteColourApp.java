package com.example.colour;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Hello world!
 */
public final class FavouriteColourApp {
    private FavouriteColourApp() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");


        final Properties config = loadConfig(args[0]);


        //Properties config = new Properties();
        //config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
        //config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        System.out.println("La clase es "+config.getProperty("rocksdb"));
        String favouritecolourinput = config.getProperty("favourite-colour-input", "favourite-colour-input");
        String userkeysandcolours= config.getProperty("user-keys-and-colours", "user-keys-and-colours");
        String  favouritecolouroutput= config.getProperty("favourite-colour-output", "favourite-colour-output");
        
        System.out.println("path de rocksdb: "+System.getenv("ROCKSDB"));
        switch (config.getProperty("rocksdb")) {
 
            // Case 1
            case "MaxWithDirect":
     
                // Print statement corresponding case
                System.out.print("rocksDB - MaxWithDirect");
                config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, customRocksDBConfigOriginal.class);
     
                // break keyword terminates the
                // code execution here itself
                break;
     
            // Case 2
            case "OnlyMax":
     
                // Print statement corresponding case
                System.out.print("rocksDB - OnlyMax");
                config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
                break;
            // Case 3
            case "ECI2":
     
                // Print statement corresponding case
                System.out.print("rocksDB - ECI2");
                config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, customRocksDBConfigECI2.class);
                break;
            default:
                System.out.print("rocksDB - Original");
                config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, customRocksDBConfigOriginal.class);
        }
        
        // JSOTO rack.aware.assignment.tags
        //config.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstream");
        ///metrics.recording.level
        config.put("metrics.recording.level", config.getProperty("metrics.recording.level", "INFO"));

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        //config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");



        StreamsBuilder builder = new StreamsBuilder();
        // Step 1: We create the topic of users keys to colours
        KStream<String, String> textLines = builder.stream(favouritecolourinput);

        KStream<String, String> usersAndColours = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase());
                // 4 - we filter undesired colours (could be a data sanitization step
                //.filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        usersAndColours.to(userkeysandcolours);

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();


        // step 2 - we read that topic as a KTable so that updates are read correctly
        
        KTable<String, String> usersAndColoursTable = builder.table(userkeysandcolours);

        // step 3 - we count the occurences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favouriteColours.toStream().to(favouritecolouroutput, Produced.with(Serdes.String(),Serdes.Long()));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        //streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        TopologyDescription description = topology.describe();
        System.out.println(description);
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


    public static Properties loadConfig( String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
          throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
          cfg.load(inputStream);
        }
        return cfg;
      }
      
}
