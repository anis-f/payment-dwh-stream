package com.stuff.stream;

import com.datastax.driver.mapping.Mapper;
import com.stuff.stream.domain.Payment;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Function;
import scala.Int;


import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class PaymentReceiver {

    public static void main(String[] args) throws Exception {


        ObjectMapper mapper = new ObjectMapper();
        Configuration conf = new Configuration();
        final StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "anisf01-mac:9092");
        properties.setProperty("group.id", "payment-stream-service");
        //Setup a Kafka Consumer on Flnk
        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>
                        ("payment", //topic
                                new SimpleStringSchema(), //Schema for data
                                properties); //connection properties

        kafkaConsumer.setStartFromEarliest();
        //Create the data stream
        DataStream<String> paymentsDataStream = streamExecutionEnvironment
                .addSource(kafkaConsumer);

        //Convert each record to an Object
        DataStream<Payment> paymentObj = paymentsDataStream
                .map(paymentJson -> {
                    //System.out.println("payment ===> " + paymentJson);
                    Payment payment = mapper.readValue(paymentJson, Payment.class);
                    return payment;
                })
                .keyBy("risk");

        //paymentObj.print();

        CassandraSink.addSink(paymentObj)
                .setHost("192.168.1.239")
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();

//        paymentObj.map(payment -> {
//            return Tuple2.of(payment.getAmount(), payment.getRisk());
//        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE)).keyBy(1).sum(1).print();

        paymentObj.map(payment -> {
            return Tuple2.of(payment.getRisk(), payment.getAmount());
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE)).keyBy(t -> {
            return t.f0;
        }).timeWindow(Time.seconds(30)).sum(1).print();

        streamExecutionEnvironment.execute("Payment receiver stream");

    }

}
