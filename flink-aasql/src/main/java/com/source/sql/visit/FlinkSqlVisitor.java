package com.source.sql.visit;

import com.source.sql.visit.sql.FSqlCall;
import com.source.sql.visit.sql.FSqlIdentifier;
import com.source.sql.visit.sql.FSqlLiteral;

import java.util.Date;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * FlinkSqlVisitor实现访问者SqlVisitor接口，定义具体的节点处理逻辑
 */
public class FlinkSqlVisitor implements SqlVisitor{
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlLiteral类型的节点
     */
    @Override
    public void visit(FSqlLiteral literal) {
        System.out.println("解析:SqlLiteral");
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlCall类型的节点
     */
    @Override
    public void visit(FSqlCall call) {
        System.out.println("解析:SqlCall");

    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlIdentifier类型的节点
     */
    @Override
    public void visit(FSqlIdentifier id) {
        System.out.println("解析:SqlIdentifier");
    }

    @Override
    public void visit() {
        System.out.println(new Date());
    }
}
