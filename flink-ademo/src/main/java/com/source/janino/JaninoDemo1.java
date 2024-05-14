package com.source.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class JaninoDemo1 {
    public static void main(String[] args) {
        String content = "System.out.println(\"测试 Janino\");";
        IScriptEvaluator evaluator = new ScriptEvaluator();
        try {
            evaluator.cook(content);
            evaluator.evaluate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
