package com.source.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: 小Q
 * @Desctription: TODO
 * @Date: Created in 2024/4/14 1:51
 * @Version: 1.0
 */


public class SocketWordCountStreamGraph {
    public static void main(String[] args) throws Exception{
        /**
         * 创建StreamExecutionEnvironment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("F:/a.txt","cache");
        /** 设置检查点的时间间隔 */
        env.enableCheckpointing(5000);
        /** 设置检查点路径*/
        env.getCheckpointConfig().setCheckpointStorage("file:///chk");
        env.setParallelism(5);
        env.setMaxParallelism(10);
        /** 读取socket数据 */
        DataStreamSource<String> fileStream =   env.socketTextStream("127.0.0.1",9999);
        /** 将数据转成小写 */
        SingleOutputStreamOperator<String> mapStream = fileStream.map(String :: toUpperCase);
        mapStream.setDescription("aaaaaaa");
        /** 按照空格切分字符串*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> flatMapStream = mapStream.flatMap(new Split());
        mapStream.setDescription("bbbbb");

        /** 分组聚合*/
        KeyedStream<Tuple2<String,Integer>,String> keyStream = flatMapStream.keyBy(value -> value.f0);
        /** 聚合*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> sumStream = keyStream.sum(1);
        //sumStream.cache();
        /** 打印 */
        DataStreamSink<Tuple2<String,Integer>> sink = sumStream.print();
        /** 执行任务 */
        env.execute("WordCount");
    }

    public static class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {

        /**
         * 按照空格切分数据
         * @param element
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String [] eles = element.split(" ");
            for(String chr : eles){
                collector.collect(new Tuple2<>(chr,1));
            }
        }
    }
}


