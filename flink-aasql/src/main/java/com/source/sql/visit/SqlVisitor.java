package com.source.sql.visit;

import com.source.sql.visit.sql.FSqlCall;
import com.source.sql.visit.sql.FSqlIdentifier;
import com.source.sql.visit.sql.FSqlLiteral;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 定义SqlVisitor访问者接口
*/
public interface SqlVisitor {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlLiteral类型的节点
    */
    void visit(FSqlLiteral literal);

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlCall类型的节点
     */
    void visit(FSqlCall call);

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlIdentifier类型的节点
     */
    void visit(FSqlIdentifier id);

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 访问解析FSqlIdentifier类型的节点
     */
    void visit();
}
