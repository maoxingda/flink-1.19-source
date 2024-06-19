package com.source.sql.demo;

import org.apache.calcite.config.Lex;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;


public class ParserDemo {
    public static void main(String[] args) throws Exception{
        String sql = "select id,name from t_user where id > 5 ";
        //String sql = "use db ";
        SqlParser.Config parserConfig =
                SqlParser.config()
                        .withParserFactory(FlinkSqlParserImpl.FACTORY)
                        .withLex(Lex.JAVA)
                        .withIdentifierMaxLength(256);

        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNodeList nodeList = parser.parseStmtList();
        System.out.println(nodeList.toString());
    }

}
