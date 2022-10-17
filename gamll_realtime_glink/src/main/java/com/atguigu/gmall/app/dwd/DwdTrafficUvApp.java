package com.atguigu.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class DwdTrafficUvApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        String sourceTopic = "dwd_traffic_page_log";
        String sinkTopic = "dwd_traffic_uv_log";
        String groupId = "dwd_traffic_uv_app";


        //getKafkaConsumer
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId);
        //addKafkaSource
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);


        KeyedStream<JSONObject, String> keyedJsonStream = kafkaStream.map(JSON::parseObject)
              .keyBy(e -> e.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> uvStream = keyedJsonStream.filter(new RichFilterFunction<JSONObject>() {
            //last visited status
            ValueStateDescriptor<String> lastVisitValueStateDesc = new ValueStateDescriptor<String>("last_visit", String.class);

            //
            ValueState<String> lastVisitValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig config = StateTtlConfig.newBuilder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();

                lastVisitValueStateDesc.enableTimeToLive(config);
                lastVisitValueState = getRuntimeContext().getState(lastVisitValueStateDesc);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                //first page
                if (lastPageId == null) {
                    //lastVisitValueState is not null
                    if (lastVisitValueState.value() != null && lastVisitValueState.value().length() > 0)
                        //curDate = lastVisitValueState
                        if (lastVisitValueState.value().equals(curDate))
                            return false;
                    lastVisitValueState.update(curDate);
                    return true;
                }
                //else(not first page)
                return false;
            }
        });

        SingleOutputStreamOperator<String> sinkStream = uvStream.map(e -> JSON.toJSONString(e));
        sinkStream.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();

    }
}
