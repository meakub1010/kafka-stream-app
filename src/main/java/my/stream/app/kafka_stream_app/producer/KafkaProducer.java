package my.stream.app.kafka_stream_app.producer;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import my.stream.app.kafka_stream_app.model.Transaction;

@Service
@RequiredArgsConstructor
public class KafkaProducer {
    @Autowired
    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final static Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public void send(Transaction message){
        kafkaTemplate.send("transactions", message.getTransactionId(), message);
        log.info("Produced transaction: Key: {}, Value: {}", message.getTransactionId(), message);
    }
}
