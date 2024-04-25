package com.source.demo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @Author: 小Q
 * @微信: yi_locus
 * @Desctription: 码界探索，不懂及时向老师提问
 * @Date: Created in 2024/4/15 20:43
 * @Version: 1.0
 */
public class Clean {

    public class Inner implements Serializable {

    }

    public static void main(String[] args) {
        Clean outer = new Clean();
        Inner inner = outer.new Inner();

        ClosureCleaner.clean(inner, ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL, false);
        //ClosureCleaner.ensureSerializable(inner);
        //System.out.println();
        System.out.println((true || false));
        //System.out.println(Function.identity());
    }


}
