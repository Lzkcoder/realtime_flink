package com.atguigu.gmall.bean;

import lombok.Data;

@Data
public class TableProcess {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkPrimaryKey;
    String sinkExtend;
}
