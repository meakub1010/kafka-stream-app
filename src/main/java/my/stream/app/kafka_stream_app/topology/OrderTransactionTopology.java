package my.stream.app.kafka_stream_app.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import my.stream.app.kafka_stream_app.model.EnrichedOrder;
import my.stream.app.kafka_stream_app.model.Transaction;
import my.stream.app.kafka_stream_app.model.Order;

@Configuration
@EnableKafkaStreams
public class OrderTransactionTopology {
    @Bean
    public KStream<String, EnrichedOrder> streamOrderTransactions(StreamsBuilder streamsBuilder) {
        KTable<String, Order> ordersTable = streamsBuilder.stream(
            "orders", 
            Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)))
            // .peek((k, v) -> {
            //     System.out.println("Order Table Stream Key: " + k + ", Value: " + v);
            // })
            .selectKey((k, order) -> order.getOrderId())
            .toTable();
        
        KTable<String, Transaction> transactionsTable = streamsBuilder.stream(
            "transactions", 
            Consumed.with(Serdes.String(), new JsonSerde<>(Transaction.class)))
            // .peek((k, v) -> {
            //     System.out.println("Transaction Table Stream Key: " + k + ", Value: " + v);
            // })
            .selectKey((k, tx) -> tx.getOrderId())
            .toTable();

        // join both tables on orderId
        KTable<String, EnrichedOrder> joinedTable = ordersTable.leftJoin(
            transactionsTable, 
            (order, transaction) -> {
                EnrichedOrder eo = new EnrichedOrder();
                eo.setOrderId(order.getOrderId());
                if(transaction != null) {
                    eo.setTransactionId(transaction.getTransactionId());
                    eo.setAmount(transaction.getAmount());
                    eo.setLocation(transaction.getLocation());
                    eo.setTimestamp(transaction.getTimestamp());
                }
                eo.setQuantity(order.getQuantity());
                eo.setUser(order.getUser());
                eo.setItem(order.getItem());

                return eo;
            }, 
            Materialized.with(Serdes.String(), new JsonSerde<>(EnrichedOrder.class)));

            KStream<String, EnrichedOrder> enrichedStream = joinedTable.toStream();

        // enrichedStream.peek((key, value) -> {
        //     System.out.println("Enriched Stream Key: " + key + ", Value: " + value);
        // });

        enrichedStream.to("order-transactions-merged");
        return enrichedStream;

    }

}
