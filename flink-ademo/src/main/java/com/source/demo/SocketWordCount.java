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
  * @授课老师(V): yi_locus
  * email: 156184212@qq.com
 * @Date: Created in 2024/4/14 1:51
 * @Version: 1.0
  */
public class SocketWordCount {
    public static void main(String[] args) throws Exception{
        /**
         * 创建StreamExecutionEnvironment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /** 读取socket数据 */
        DataStreamSource<String> fileStream =   env.socketTextStream("127.0.0.1",9999);
        /** 将数据转成小写 */
        SingleOutputStreamOperator<String> mapStream = fileStream.map(String :: toLowerCase);
        /** 按照空格切分字符串*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> flatMapStream = mapStream.flatMap(new Split());
        /** 分组聚合*/
        KeyedStream<Tuple2<String,Integer>,String> keyStream = flatMapStream.keyBy(value -> value.f0);
        /** 聚合*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> sumStream = keyStream.sum(1);
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


