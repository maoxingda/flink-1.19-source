package com.source.sql.visit;

import com.source.sql.visit.sql.FQqlNode;
import com.source.sql.visit.sql.FSqlCall;
import com.source.sql.visit.sql.FSqlIdentifier;
import com.source.sql.visit.sql.FSqlLiteral;

/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 构建测试需要的节点类
*/
public class SqlNodeObj {
    FQqlNode[] nodes;
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化FSqlCall、FSqlIdentifier、FSqlLiteral节点
    */
    public SqlNodeObj(){
        nodes = new FQqlNode[]{new FSqlCall(),new FSqlIdentifier(),
        new FSqlLiteral()};
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * SqlVisitor具体实例作为参数循环访问node节点
    */
    public void accept(SqlVisitor sqlVisitor){
        for(int i =0; i< nodes.length ; i++){
            nodes[i].accept(sqlVisitor);
        }
    }
}
