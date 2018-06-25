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

    public static ExecutorService cachedThreadPoolRun(Runnable runnable){
        if(cachedThreadPool==null){
            cachedThreadPool= Executors.newCachedThreadPool();
            cachedThreadPool.submit(runnable);
            return cachedThreadPool;
        }
        return cachedThreadPool;
    }

    public static ExecutorService fixedThreadPoolRun(Runnable runnable){
        if (fixedThreadPool==null){
            fixedThreadPool=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            fixedThreadPool.submit(runnable);
        }
        return fixedThreadPool;
    }

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
            executorService = ThreadPoolUtils.scheduledThreadPoolRun(runnable,1,TimeUnit.SECONDS);
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
