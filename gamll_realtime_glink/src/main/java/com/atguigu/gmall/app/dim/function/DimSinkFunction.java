package com.atguigu.gmall.app.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.constant.GmallConfig;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;
import java.util.Set;

public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        //1 把 jsonObj解析为一个sql  upsert
        // upsert  into  ${sink_table }(字段名1，.。。) values(字段值1，  )
        // 提取data的键值对
        StringBuilder fieldNameStringBuilder =new StringBuilder("(");
        StringBuilder fieldValueStringBuilder =new StringBuilder("(");

        JSONObject dataJsonObj = jsonObject.getJSONObject("data");

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();

        for (Map.Entry<String, Object> entry : entries) {
            String fieldName = entry.getKey();
            Object fieldValue =  entry.getValue();
            if(fieldNameStringBuilder.length()>1){
                fieldNameStringBuilder.append(",");
                fieldValueStringBuilder.append(",");
            }
            fieldNameStringBuilder.append(fieldName);
            fieldValueStringBuilder.append("'"+fieldValue+"'");

        }
        fieldNameStringBuilder.append(")");
        fieldValueStringBuilder.append(")");

        String sinkTable = jsonObject.getString("sink_table");
        StringBuilder upsertSQL=new StringBuilder("upsert into "+ GmallConfig.PHOENIX_SCHEMA+"."+sinkTable)
              .append(fieldNameStringBuilder).append("values").append(fieldValueStringBuilder);

        System.out.println(upsertSQL);

        // 2 把upsert  交给PhoenixUtil执行
        PhoenixUtil.executeSql(upsertSQL.toString());
    }
}
