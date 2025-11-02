package my.stream.app.kafka_stream_app.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import my.stream.app.kafka_stream_app.model.Transaction;

@Service
public class KafkaConsumer {
    @KafkaListener(topics = {"transactions"}, groupId = "kstream-listener-group")
    public void listen(Transaction message) {
        System.out.println("Received Message: " + message);
    }

    // @KafkaListener(topics = "orders", groupId = "kstream-listener-group")
    // public void listenOrders(Order order) {
    //     System.out.println("Received order: " + order);
    // }
}
