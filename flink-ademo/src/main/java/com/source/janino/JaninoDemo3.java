package com.source.janino;

import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class JaninoDemo3 {
    public static void main(String[] args) {
        IScriptEvaluator evaluator = new ScriptEvaluator();
        String express = "public class ScriptDemo {\n"
                + "    public static void main(String[] args) {\n"
                + "        ScriptDemo.println1();\n"
                + "    }\n"
                + "    public static void println1(){\n"
                + "        System.out.println(\"运行 println1\");\n"
                + "    }\n"
                + "}";
        try {
            evaluator.cook(express);
            evaluator.evaluate(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
