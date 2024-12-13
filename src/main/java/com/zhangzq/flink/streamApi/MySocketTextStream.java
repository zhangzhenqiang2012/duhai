package com.zhangzq.flink.streamApi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lenovo
 * @version 1.0
 * @description: socket文件输入流接口模版
 * @date 2024/12/13 14:08
 */
public class MySocketTextStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds = env.socketTextStream("192.168.32.131", 6666);
        ds.flatMap((String line, Collector<String> out) -> {
                    for (String s : line.split(" ")) {
                        out.collect(s);
                    }
                }).returns(Types.STRING).map(t -> Tuple2.of(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute("lambda");
    }
}
