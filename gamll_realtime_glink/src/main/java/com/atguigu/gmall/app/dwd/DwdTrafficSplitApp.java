package com.atguigu.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficSplitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        String topic = "topic_log";
        String groupId = "dwd_traffic_split_app";

        DataStreamSource<String> kafkaDstream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId)).setParallelism(1);

        env.setStateBackend(new HashMapStateBackend());

        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        OutputTag<String> dirtyDataOutputTag = new OutputTag<String>("dirty_data") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDstream = kafkaDstream.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(dirtyDataOutputTag, value);
                }
            }
        });

        DataStream<String> dirtyDataStream = jsonObjDstream.getSideOutput(dirtyDataOutputTag);

        dirtyDataStream.print("dirty_data");

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjDstream.keyBy(e -> e.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> mapedDstream = jsonObjectStringKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {


            ValueState<String> firstVisitDateState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> firstVisitDateStateDesc =
                      new ValueStateDescriptor<String>("first_visit_date_state", String.class);

                firstVisitDateState = getRuntimeContext().getState(firstVisitDateStateDesc);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取本条信息的is_new 字段
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                //本条信息的时间戳
                Long ts = jsonObject.getLong("ts");
                //时间戳转换为年月日
                String curDate = DateFormatUtil.toDate(ts);
                //前端传入is_new is 1
                if (isNew.equals("1")) {
                    // 如果后端登录状态为空
                    if (firstVisitDateState.value() == null || firstVisitDateState.value().length() == 0) {
                        //更新后端状态
                        firstVisitDateState.update(curDate);
                        //如果不为空
                    } else {
                        //判断是否和后端状态为同一天
                        if (!curDate.equals(firstVisitDateState.value())) {
                            //not the same day
                            //new date should change state
                            jsonObject.getJSONObject("common").put("is_new", "0");
                        }
                    }
                    //if is_new is 0
                } else if (firstVisitDateState.value() == null || firstVisitDateState.value().length() == 0) {
                    String subOneDay = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    firstVisitDateState.update(subOneDay);
                }
                return jsonObject;
            }
        });

        //启动侧输出流
        OutputTag<String> startOutputTag = new OutputTag<>("start");
        //动作侧输出流
        OutputTag<String> actionOutputTag = new OutputTag<>("action");
        //曝光侧输出流
        OutputTag<String> displayOutputTag = new OutputTag<>("display");
        //错误侧输出流
        OutputTag<String> errOutputTag = new OutputTag<>("err");

        SingleOutputStreamOperator<String> splitDstream = mapedDstream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject errJsonObject = jsonObject.getJSONObject("err");
                //if errJsonObject is not null then side-output errJsonObject
                if (errJsonObject != null) {
                    ctx.output(errOutputTag, jsonObject.toJSONString());
                } else {
                    JSONObject startJsonObject = jsonObject.getJSONObject("start");
                    //if startJsonObject is not null then side-output startJsonObject
                    if (startJsonObject != null) {
                        ctx.output(startOutputTag, startJsonObject.toJSONString());
                    } else {
                        JSONArray actionJsonArr = jsonObject.getJSONArray("actions");
                        if (actionJsonArr != null) {
                            for (int i = 0; i < actionJsonArr.size(); i++) {
                                JSONObject actionJsonObject = actionJsonArr.getJSONObject(i);
                                JSONObject newActionObj = new JSONObject();
                                newActionObj.put("page", actionJsonObject.getString("page"));
                                newActionObj.put("common", actionJsonObject.getString("common"));
                                newActionObj.put("action", actionJsonObject);
                                ctx.output(actionOutputTag, newActionObj.toJSONString());
                            }
                        }
                        JSONArray displayJsonArr = jsonObject.getJSONArray("displays");
                        // if displayJsonArr is not null
                        if (displayJsonArr != null) {
                            for (int i = 0; i < displayJsonArr.size(); i++) {
                                JSONObject displayJsonObject = displayJsonArr.getJSONObject(i);
                                JSONObject newDisplayObj = new JSONObject();
                                newDisplayObj.put("page", displayJsonObject.getString("page"));
                                newDisplayObj.put("common", displayJsonObject.getString("common"));
                                newDisplayObj.put("display", displayJsonObject);
                                newDisplayObj.put("ts", jsonObject.getLong("ts"));
                                ctx.output(displayOutputTag, newDisplayObj.toJSONString());
                            }
                        }
                        jsonObject.remove("displays");
                        jsonObject.remove("actions");
                        out.collect(jsonObject.toJSONString());
                    }
                }
            }
        });
        //main stream
        splitDstream.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));

        DataStream<String> errSideOutput = splitDstream.getSideOutput(errOutputTag);
        errSideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_err_log"));
        errSideOutput.print("err");

        DataStream<String> startSideOutput = splitDstream.getSideOutput(startOutputTag);
        startSideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        startSideOutput.print("start");

        DataStream<String> actionSideOutput = splitDstream.getSideOutput(actionOutputTag);
        actionSideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));
        actionSideOutput.print("action");

        DataStream<String> displaySideOutput = splitDstream.getSideOutput(displayOutputTag);
        displaySideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        displaySideOutput.print("display");

        env.execute();
    }
}











