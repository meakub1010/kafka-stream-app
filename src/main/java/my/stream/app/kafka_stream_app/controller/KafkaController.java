package my.stream.app.kafka_stream_app.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import my.stream.app.kafka_stream_app.model.Transaction;
import my.stream.app.kafka_stream_app.producer.KafkaProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {
    private final KafkaProducer kafkaProducer;

    @PostMapping("/transaction")
    public String transaction(@RequestBody Transaction trans) {
        kafkaProducer.send(trans);
        
        return "Message sent to Kafka Topic";
    }
    
    
}
