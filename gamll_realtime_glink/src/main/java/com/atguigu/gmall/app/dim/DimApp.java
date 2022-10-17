package com.atguigu.gmall.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.app.dim.function.DimSinkFunction;
import com.atguigu.gmall.app.dim.function.TableProcessFunction;
import com.atguigu.gmall.bean.TableProcess;
import com.atguigu.gmall.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //部署时注释掉
        env.setParallelism(4);

        String topic = "topic_db";
        String groupId = "dim_app_group";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        DataStreamSource<String> kafkaDataStreamSource = env.addSource(kafkaConsumer);
        kafkaDataStreamSource.print("maxwell");

        SingleOutputStreamOperator<JSONObject> jsonObjDstream = kafkaDataStreamSource.map(e -> JSON.parseObject("e"));
        SingleOutputStreamOperator<JSONObject> gmallDstream = jsonObjDstream.filter(e -> e.getString("database").equals("gmall"));

        //2 维度配置表
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
              .hostname("hadoop102")
              .port(3306)
              .databaseList("gmall_realtime_config")
              .tableList("gmall_realtime_config.table_process")
              .username("root")
              .password("000000")
              .deserializer(new JsonDebeziumDeserializationSchema())
              .build();

        DataStreamSource<String> tableProcess = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "table_process");

        tableProcess.print("flinkCDC");

        MapStateDescriptor<String, TableProcess> tableProcessState = new MapStateDescriptor<>("table_process_state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = tableProcess.broadcast(tableProcessState);

        BroadcastConnectedStream<JSONObject, String> connect = gmallDstream.connect(broadcast);

        SingleOutputStreamOperator<JSONObject> process = connect.process(new TableProcessFunction(tableProcessState));

        process.print();

        process.addSink(new DimSinkFunction());


        env.execute();
    }
}
