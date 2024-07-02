package com.source.sql.visit;

public class Demo {
    public static void main(String[] args) {
        //构建SqlVisitor具体实现类FlinkSqlVisitor
        SqlVisitor sqlVisitor = new FlinkSqlVisitor();
        //构建Node测试类
        SqlNodeObj node = new SqlNodeObj();
        //解析node节点
        node.accept(sqlVisitor);
    }
}
