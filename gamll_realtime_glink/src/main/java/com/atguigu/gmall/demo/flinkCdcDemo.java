package com.atguigu.gmall.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flinkCdcDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
              .hostname("hadoop102")
              .port(3306)
              .databaseList("gmall_realtime_config")
              .tableList("gmall_realtime_config.customer")
              .username("root")
              .password("000000")
              .deserializer(new JsonDebeziumDeserializationSchema())
              .build();

        DataStreamSource<String> customerSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "customer_source");

        customerSource.print();

        env.execute();
    }
}
