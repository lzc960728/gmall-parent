package com.lzc.gmall.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lzc.gmall.canal.client.utils.MykafkaSender;
import com.lzc.gmall.common.GmallConstants;

import java.util.List;

/**
 * @program: gmall-parent
 * @ClassName CanalHanlder
 * @description:
 * @author: lzc
 * @create: 2020-03-10 23:58
 * @Version 1.0
 **/
public class CanalHanlder {
    // 表名
    private String tableName;
    // Sql类型
    private CanalEntry.EventType eventType;
    // 数据集合
    private List<CanalEntry.RowData> rowDatasList;

    public CanalHanlder(String tableName, CanalEntry.EventType entryType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = entryType;
        this.rowDatasList = rowDatasList;
    }

    public void handle() {
        if (tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_ORDER);
            }
        }
    }

    /**
     * 发送Kafka
     *
     * @param rowData
     * @param topic
     */
    private void sendKafka(CanalEntry.RowData rowData, String topic) {
        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : afterColumnsList) {
            System.out.println(column.getName() + "------>" + column.getValue());
            // 发送数据到对应的topic中
            jsonObject.put(column.getName(), column.getValue());
        }
        String rowJson = jsonObject.toJSONString();
        MykafkaSender.send(topic,rowJson);
    }
}
