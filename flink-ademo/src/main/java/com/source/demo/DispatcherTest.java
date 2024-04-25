package com.source.demo;

public class DispatcherTest {
    public static void main(String[] args) {
        JobManagerRunnerTest jobManagerRunner = new JobManagerRunnerTest();
        jobManagerRunner.start();
        jobManagerRunner.getResultFuture().handle((result, exception) -> {
            if (exception != null) {
                System.err.println("An error occurred: " + exception.getMessage());
            } else {
                System.out.println("The result is: " + result);
            }
            return null; // handle 方法不需要返回结果，可以返回 null 或其他任意值
        });
    }
}
