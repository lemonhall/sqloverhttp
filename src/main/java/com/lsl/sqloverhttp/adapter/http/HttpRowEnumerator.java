package com.lsl.sqloverhttp.adapter.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.calcite.linq4j.Enumerator;

import java.util.LinkedList;
import java.util.Set;

public class HttpRowEnumerator<E> implements Enumerator<E> {
    //runtime
    //新建一个整形的链表先用来模拟数据
    private final LinkedList<E> bufferedRecords = new LinkedList<E>();
    private E curRecord;
    private JsonArray dataArray;

    public HttpRowEnumerator(JsonArray dataArray) {
        pullRecords(dataArray);
    }

    //这个是返回当前行的一个对象数组的函数
    //正确的写法应该有行的对象转换过程
    //return rowConverter.toRow(curRecord);
    // Kafka那边是这么写的转换器，可以参考
    //    /**
    //     * Parse and reformat Kafka message from consumer, to align with row schema
    //     * defined as {@link #rowDataType(String)}.
    //     * @param message, the raw Kafka message record;
    //     * @return fields in the row
    //     */
    //    @Override public Object[] toRow(final ConsumerRecord<byte[], byte[]> message) {
    //        Object[] fields = new Object[5];
    //        fields[0] = message.partition();
    //        fields[1] = message.timestamp();
    //        fields[2] = message.offset();
    //        fields[3] = message.key();
    //        fields[4] = message.value();
    @Override
    public E current() {
        return curRecord;
    }


    //从数据源当中取出一个元素，并且返回ture的过程
    @Override
    public boolean moveNext() {
        if(bufferedRecords.size()!=0) {
            curRecord = bufferedRecords.removeFirst();
            return true;
        }else {
            return false;
        }
    }

    //相当于是从数据源当中获取数据的过程，这个从http的角度来说
    //其实可以看成读取整个json的数组到内存里并初始化的过程，这相当于读取了整个表
    private void pullRecords(JsonArray dataArray) {
        for(int i=0;i<dataArray.size();i++) {
            //取一个元素出来
            JsonObject item = dataArray.get(i).getAsJsonObject();
            //取得这一行里的所有key
            Set<String> colKeySet = item.keySet();
            Object[] row;
            //取得这个key组合下的所有的值
            for (String keyName : colKeySet) {
                String value = item.get(keyName).getAsString();
                System.out.println("row value:"+value);
            }
        }
        bufferedRecords.add((E) new Object[]{"001","余柠"});
        bufferedRecords.add((E) new Object[]{"002","小倩"});
        bufferedRecords.add((E) new Object[]{"003","宁采臣"});
        bufferedRecords.add((E) new Object[]{"004","马道士"});
        bufferedRecords.add((E) new Object[]{"005","土匪甲"});
    }

    @Override
    public void reset() {
        System.out.println("报错了兄弟，不支持此操作");
    }

    @Override
    public void close() {
    }
}
