package com.source.sql.basic;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * // 定义一个简单的标量函数，实现字符串长度计算
*/
public class StringLengthFunction extends ScalarFunction {
        private String s;
        public StringLengthFunction() {
            this.s = "aaaa";
        }
        public StringLengthFunction(String s) {
            this.s = s;
        }

        public int eval(String s) {
            return s.length();
        }
    }
