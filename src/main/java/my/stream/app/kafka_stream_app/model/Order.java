package my.stream.app.kafka_stream_app.model;

import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class Order implements Serializable {
    @JsonProperty("order_id")
    private String orderId;
    private String user;
    private String item;
    private int quantity;

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", user='" + user + '\'' + 
                ", item='" + item + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
