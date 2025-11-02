package my.stream.app.kafka_stream_app.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class Transaction {
    String transactionId;
    String orderId;
    double amount;
    String timestamp;
    String location;
}
