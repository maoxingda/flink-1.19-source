package com.source.demo;

import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;

public class JobManagerRunnerTest {
    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();
    boolean flag = true;
    public void start(){
        /**已完成的CompletableFuture<Void> 返回值为null */
        // 使用 thenCompose 将两个异步操作链接在一起
        sequentialOperation.thenCompose(str -> {
            // 第二个异步操作：根据第一个操作的结果获取一个整数，并生成最终的字符串
            return CompletableFuture.supplyAsync(() -> {
                // 模拟另一个耗时操作
                try {
                    resultFuture.complete(null);
                    Thread.sleep(5000); // 假设这里也是一个耗时操作
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                return "World!"; // 假设这是基于第一个操作结果计算得到的整数值对应的字符串
            });
        });


    }
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }}
