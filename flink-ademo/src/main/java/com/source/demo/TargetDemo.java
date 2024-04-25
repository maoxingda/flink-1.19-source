package com.source.demo;

public class TargetDemo {

    public static void main(String[] args) {
        int targetCount = 5;
        int sourceCount = 5;
        for (int index = 0; index < targetCount; index++) {

            int start = index * sourceCount / targetCount;
            int end = (index + 1) * sourceCount / targetCount;
            System.out.println("start="+start + ",end="+end);
        }
    }
}
