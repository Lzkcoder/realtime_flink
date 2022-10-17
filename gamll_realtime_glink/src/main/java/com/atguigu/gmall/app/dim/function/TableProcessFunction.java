package com.atguigu.gmall.app.dim.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TableProcess;
import com.atguigu.gmall.constant.GmallConfig;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    MapStateDescriptor<String, TableProcess> tableMapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> descriptor) {
        this.tableMapStateDescriptor = descriptor;
    }

    @Override
    public void processElement(JSONObject gmallDataJson, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);

        String tableName = gmallDataJson.getString("table");
        TableProcess tableProcess = broadcastState.get(tableName);
        String type = gmallDataJson.getString("type");
        if (tableProcess!=null && !type.equals("bootstrap-start")&&!type.equals("bootstrap-complete")){
            String sinkColumns = tableProcess.getSinkColumns();
            String[] splits = sinkColumns.split(",");

            HashSet<String> cols = new HashSet<>(Arrays.asList(splits));

            JSONObject jsonObject = gmallDataJson.getJSONObject("data");

            Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();

            entrySet.removeIf(e -> !cols.contains(e.getKey()));

            gmallDataJson.put("sink_table",tableProcess.getSinkTable());

            out.collect(gmallDataJson);
        }
    }

    @Override
    public void processBroadcastElement(String tableProcessJson, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject tableProcessJsonObject = JSON.parseObject(tableProcessJson);

        String operator = tableProcessJsonObject.getString("op");

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);

        if (!operator.equals("d")){
            TableProcess after = tableProcessJsonObject.getObject("after", TableProcess.class);

            checkTable(after);
            broadcastState.put(after.getSourceTable(),after);
        }else {
            TableProcess before = tableProcessJsonObject.getObject("before", TableProcess.class);
            broadcastState.remove(before.getSourceTable());
        }
    }

    private void checkTable(TableProcess tableProcess) {
        StringBuilder craeteTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
              .append(GmallConfig.PHOENIX_SCHEMA + ".")
              .append(tableProcess.getSinkTable())
              .append("(");

        String[] splits = tableProcess.getSinkColumns().split(",");

        List<String> strings = Arrays.asList(splits);
        craeteTableSql
              .append(strings.get(0))
              .append(" varchar ");
        if (strings.get(0).equals(tableProcess.getSinkPrimaryKey()))
            craeteTableSql.append("primary key");
        strings.remove(0);

        for (String string : strings) {
            craeteTableSql.append(",").append(string);
        }
        craeteTableSql.append(")");

        PhoenixUtil.executeSql(craeteTableSql.toString());
    }

}
