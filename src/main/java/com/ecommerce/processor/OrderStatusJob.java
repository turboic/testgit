package com.ecommerce.processor;

import com.ecommerce.event.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;

public class OrderStatusJob {

    // ==================== 新增静态转换器类 ====================
    public static class OrderStatusConverter implements ClickHouseSink.ClickHouseRowConverter<OrderStatusRecord>, Serializable {
        @Override
        public Object[] convert(OrderStatusRecord element, SinkWriter.Context context) {
            return new Object[]{
                    element.getOrderId(),
                    element.getStatus().name(),
                    element.getUpdateTime()
            };
        }

        @Override
        public int getFieldCount() {
            return 3;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 配置Kafka数据源 ====================
        KafkaSource<OrderEvent> kafkaSource = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers("10.7.215.182:9092")
                .setTopics("avro_demo")
                .setGroupId("avro-demo-flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        OrderEvent.class,
                        "http://10.7.215.182:8081"
                ))
                // 移除冲突的 offset 配置
                //.setProperty("auto.offset.reset", "earliest")
                .build();

        DataStream<OrderEvent> orderEvents = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp().toEpochMilli()),
                "Kafka-Order-Events"
        ).returns(TypeInformation.of(OrderEvent.class)); // 显式指定类型

        // ==================== 2. 处理逻辑 ====================
        SingleOutputStreamOperator<OrderStatusRecord> statusStream = orderEvents
                .keyBy(OrderEvent::getOrderId)
                .process((KeyedProcessFunction)new OrderStatusProcessor())
                .returns(OrderStatusRecord.class); // 显式指定输出类型

        // ==================== 3. Sink配置 ====================
        ClickHouseSink<OrderStatusRecord> clickHouseSink = new ClickHouseSink.Builder<OrderStatusRecord>()
                .setURL("jdbc:clickhouse://10.10.10.136:8123/ecommerce")
                .setTableName("order_status")
                .setUsername("default")
                .setBatchSize(1000)
                .setBatchIntervalMs(5000)
                .setRowConverter(new OrderStatusConverter()) // 使用静态转换器
                .build();

        statusStream.sinkTo(clickHouseSink).name("ClickHouse-Sink");

        env.execute("Order-Status-Real-Time-Processor");
    }

    // ==================== 处理函数 ====================
    public static class OrderStatusProcessor extends KeyedProcessFunction<String, OrderEvent, OrderStatusRecord> {
        private transient MapState<String, OrderStatus> orderState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, OrderStatus> descriptor = new MapStateDescriptor<>(
                    "order-state",
                    TypeInformation.of(String.class),
                    TypeInformation.of(OrderStatus.class) // 显式指定类型
            );
            orderState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(OrderEvent event,
                                   Context context,
                                   Collector<OrderStatusRecord> collector) throws Exception {
            String orderId = (String)event.getOrderId();
            OrderStatus currentStatus = orderState.get(orderId);

            // 状态更新逻辑保持不变...
            // （确保所有类型转换安全）

            orderState.put(orderId, currentStatus);
            collector.collect(new OrderStatusRecord(
                    orderId,
                    currentStatus,
                    event.getTimestamp().toEpochMilli()
            ));
        }
    }

    // ==================== 数据类 ====================
    public static class OrderStatusRecord implements Serializable {
        private String orderId;
        private OrderStatus status;
        private long updateTime;

        // 必须有无参构造器
        public OrderStatusRecord() {}

        public OrderStatusRecord(String orderId, OrderStatus status, long updateTime) {
            this.orderId = orderId;
            this.status = status;
            this.updateTime = updateTime;
        }

        // Getter/Setter 方法
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }

        public OrderStatus getStatus() { return status; }
        public void setStatus(OrderStatus status) { this.status = status; }

        public long getUpdateTime() { return updateTime; }
        public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
    }

    // ==================== 枚举类 ====================
    public enum OrderStatus {
        CREATED, PAID, SHIPPED, COMPLETED, CANCELED, REFUNDING
    }
}
