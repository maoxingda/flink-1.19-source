package com.source.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 小Q
 * @Desctription: TODO
 * @Date: Created in 2024/4/14 1:51
 * @Version: 1.0
 */


public class SocketWordCountOperatorState {
    public static void main(String[] args) throws Exception{
        /**
         * 创建StreamExecutionEnvironment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("./a_conf/a.txt","cache");

        /** 设置检查点的时间间隔 */
        //需要开启 Checkpoint 机制
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //需要开启持久化的路径  可选hdfs 本地
        env.getCheckpointConfig().setCheckpointStorage("file:///H:/chk");


        env.setParallelism(2);
        env.setMaxParallelism(2);

        /** 读取socket数据 */
        DataStreamSource<String> dataStreamSource =   env.socketTextStream("127.0.0.1",9999);
        DataStream<String> dataStream = dataStreamSource.map(new StateMapFunction());
        dataStream.print();
        env.execute();
    }

}


class StateMapFunction implements MapFunction<String,String> , CheckpointedFunction {

    ListState<String> listState;

    //正常的处理逻辑
    @Override
    public String map(String value) throws Exception {
        listState.add(value);
        Iterable<String> strings = listState.get();
        StringBuilder sb = new StringBuilder();
        for (String string : strings) {
            sb.append(string);
        }
        //写一个异常
        return sb.toString();
    }

    //持久化之前会调用的方法
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();
        System.out.println("执行快照!!!!!"+ checkpointId);
    }

    //算子的任务在启动之前,会调用下面的方法,为用户的状态初始化
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //context 获取状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        //定义一个昨天存储结构的描述器
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("保存字符串", String.class);
        //获取状态存储器 中获取容器来存储器
        //getListState 方法还会加载之前存储的状态数据
        listState = operatorStateStore.getListState(listStateDescriptor);
    }
}


