package my.stream.app.kafka_stream_app.model;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class EnrichedOrder {
    String orderId;
    String user;
    String item;
    int quantity;
    String transactionId;
    double amount;
    String timestamp;
    String location;
}
