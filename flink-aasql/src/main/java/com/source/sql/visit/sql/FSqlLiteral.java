package com.source.sql.visit.sql;

import com.source.sql.visit.SqlVisitor;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 实现FQqlNode接口类对应Flink SQL中的SqlLiteral
 */
public class FSqlLiteral extends FQqlNode {
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过SqlVisitor具体实现来访问树结构中的节点
     */
    @Override
    public void accept(SqlVisitor visit) {
        visit.visit(this);
    }



}
