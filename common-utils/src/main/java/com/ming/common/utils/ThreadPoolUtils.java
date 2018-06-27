package com.ming.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by limin on 2018/6/25.
 */
public class ThreadPoolUtils {
    static ExecutorService cachedThreadPool;
    static ExecutorService fixedThreadPool;
    static ScheduledExecutorService scheduledThreadPool;
    static ExecutorService singleThreadPool;

    /**
     * CachedThreadPool 是通过 java.util.concurrent.Executors 创建的 ThreadPoolExecutor 实例。
     * 这个实例会根据需要，在线程可用时，重用之前构造好的池中线程。
     * 这个线程池在执行 大量短生命周期的异步任务时（many short-lived asynchronous task），
     * 可以显著提高程序性能。调用 execute 时，可以重用之前已构造的可用线程，如果不存在可用线程，
     * 那么会重新创建一个新的线程并将其加入到线程池中。
     * 如果线程超过 60 秒还未被使用，就会被中止并从缓存中移除。因此，线程池在长时间空闲后不会消耗任何资源。
     * @param runnable
     * @return
     */
    public static ExecutorService cachedThreadPoolRun(Runnable runnable){
        if(cachedThreadPool==null){
                cachedThreadPool= Executors.newCachedThreadPool();
            cachedThreadPool.submit(runnable);
            return cachedThreadPool;
        }
        return cachedThreadPool;
    }

    /**FixedThreadPool 是通过 java.util.concurrent.Executors 创建的 ThreadPoolExecutor 实例。
     * 这个实例会复用 固定数量的线程 处理一个 共享的无边界队列 。任何时间点，
     * 最多有 nThreads 个线程会处于活动状态执行任务。如果当所有线程都是活动时，
     * 有多的任务被提交过来，那么它会一致在队列中等待直到有线程可用。如果任何线程在执行过程中因为错误而中止，
     * 新的线程会替代它的位置来执行后续的任务。
     * 所有线程都会一致存于线程池中，直到显式的执行 ExecutorService.shutdown() 关闭。
     */

    public static ExecutorService fixedThreadPoolRun(Runnable runnable){
        if (fixedThreadPool==null){
            fixedThreadPool=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            fixedThreadPool.submit(runnable);
        }
        return fixedThreadPool;
    }

    /**
    SingleThreadPool 是通过 java.util.concurrent.Executors 创建的 ThreadPoolExecutor 实例。
    这个实例只会使用单个工作线程来执行一个无边界的队列。（注意，如果单个线程在执行过程中因为某些错误中止，
    新的线程会替代它执行后续线程）。它可以保证认为是按顺序执行的，任何时候都不会有多于一个的任务处于活动状态。
    和 newFixedThreadPool(1) 的区别在于，如果线程遇到错误中止，它是无法使用替代线程的
     */
   public static ExecutorService scheduledThreadPoolRun(Runnable runnable, long delay, TimeUnit  unit){
        if(scheduledThreadPool==null){
            scheduledThreadPool=Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
            scheduledThreadPool.schedule(runnable,delay,unit);
        }
        return scheduledThreadPool;
    }

    public static ExecutorService singleThreadPoolRun(Runnable runnable) {
        if (singleThreadPool == null)
            singleThreadPool = Executors.newSingleThreadExecutor();
        singleThreadPool.submit(runnable);
        return singleThreadPool;
    }


    public static void main(String args[]){
        final Long startTime = System.currentTimeMillis();
        ExecutorService executorService = null;
        int index=0;
        for (int i = 0; i < 1000000; i++) {
            int finalI = i;
            Runnable runnable=new Runnable() {
                @Override
                public void run() {
                    System.out.println(finalI);
                }
            };
            executorService = ThreadPoolUtils.scheduledThreadPoolRun(runnable,10,TimeUnit.SECONDS);
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        while (true) {
            if (executorService.isTerminated()) {
                System.out.println("所有的子线程都结束了！");
                break;
            }
        }

        final Long stopTime = System.currentTimeMillis();
        System.out.println("---------------------------");
        System.out.println(stopTime - startTime);
    }

}
