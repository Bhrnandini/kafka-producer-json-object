package com.reactive.kafkaProducer.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;


public class ProducerWithObject
{
    private static final Logger log = LoggerFactory.getLogger(ProducerWithObject.class);

    public static void main(String[] args) {

        String username = "username";
        String password = "username";
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"username",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG,"all",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "username",
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "username",
                SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";",
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL"
        );
        var options = SenderOptions.<String,Book>create(producerConfig);



   var flux = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .map( value -> new Book(value,value,value))
                .map( event -> new ProducerRecord<String,Book>("topic_name","abc", event))
                .map(pr -> SenderRecord.create(pr, pr.key()));



        var sender = KafkaSender.create(options);
        sender.send(flux)
                .doOnNext(
                        result -> {
                            RecordMetadata metadata = result.recordMetadata();
                            log.info("correlation id is : {}", result.correlationMetadata());
                            log.info("topic is : {}", metadata.topic());
                            log.info("partition is : {}", metadata.partition());
                            log.info("offset is : {}", metadata.offset());
                            log.info("topic is : {}", metadata.toString());
                        }
                )
                .subscribe();
    }
}
