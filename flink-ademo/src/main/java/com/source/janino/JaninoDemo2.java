package com.source.janino;

import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class JaninoDemo2 {
    public static void main(String[] args) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator();
        String express = "1+1+2+3";
        try {
            evaluator.cook(express);
            Object res = evaluator.evaluate();
            System.out.println(express+"计算结果=" + res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
