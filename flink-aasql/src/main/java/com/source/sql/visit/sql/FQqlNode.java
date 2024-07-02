package com.source.sql.visit.sql;

import com.source.sql.visit.SqlVisitor;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 定义一个树型结构最上层FQqlNode类，对应Flink SQL中的QqlNode
*/
public abstract class FQqlNode {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 定义accept方法参数SqlVisitor
    */
    public abstract void accept(SqlVisitor visit);

}
