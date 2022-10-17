package com.atguigu.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DwdTrafficJumpOutApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String sourceTopic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_out_app";

        //FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId);

        //获取kafka
        KeyedStream<JSONObject, String> midKeyedDstream = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
              //转换为JSONObject
              .map(JSON::parseObject)
              //添加水位线
              .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                    .withTimestampAssigner(
                          (element, timestamp) -> element.getLong("ts")
                    )
              )
              //根据mid(设备号)分组
              .keyBy(e -> e.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> jumpOutPattern = Pattern.<JSONObject>begin("first").
              where(new SimpleCondition<JSONObject>() {
                  @Override
                  public boolean filter(JSONObject jsonObject) throws Exception {
                      String lastPageId = jsonObject.getJSONObject("common").getString("last_page_id");
                      return lastPageId == null || lastPageId.length() == 0;
                  }
              }).next("second")
              .where(new SimpleCondition<JSONObject>() {
                  @Override
                  public boolean filter(JSONObject jsonObject) throws Exception {
                      String lastPageId = jsonObject.getJSONObject("common").getString("last_page_id");
                      return lastPageId == null || lastPageId.length() == 0;
                  }
              }).within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(midKeyedDstream, jumpOutPattern);


    }
}
