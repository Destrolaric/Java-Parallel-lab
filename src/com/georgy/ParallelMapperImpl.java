package com.georgy;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParallelMapperImpl implements ParallelMapper {
    private final Queue<Job<?, ?>> jobQueue = new LinkedList<>();
    private final List<Thread> threads = new ArrayList<>();

    public ParallelMapperImpl(int threadsNumber) {
        if (threadsNumber < 1) {
            throw new IllegalArgumentException("Illegal threads number " + threads);
        }

        for (int i = 0; i < threadsNumber; ++i) {
            threads.add(new Thread(new JobExecutor()));
        }
        threads.forEach(Thread::start);
    }

    /**
     * Maps function {@code function} over specified {@code args}. Mapping for each element performs
     * in parallel.
     *
     * @param function function
     * @param args args
     * @throws InterruptedException if calling thread was interrupted
     */
    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> args)
            throws InterruptedException {
        List<Job<T, R>> jobList = new ArrayList<>();
        SyncedClock counter = new SyncedClock(args.size());
        args.forEach(arg -> jobList.add(new Job<>(function, arg, counter)));

        synchronized (jobQueue) {
            jobQueue.addAll(jobList);
            jobQueue.notify();
        }

        counter.waitUntilFinish();
        return jobList.stream().map(Job::getResult).collect(Collectors.toList());
    }

    @Override
    public void close() {
        threads.forEach(Thread::interrupt);
        synchronized (jobQueue) {
            jobQueue.clear();
        }
    }

    private static class Job<T, R> {
        private final Function<? super T, ? extends R> function;
        private final T argument;
        private final SyncedClock clock;
        private R result;

        public Job(Function<? super T, ? extends R> function, T arg, SyncedClock counter) {
            this.function = function;
            this.argument = arg;
            this.clock = counter;
        }

        public void run() {
            result = function.apply(argument);
            clock.incAndNotifyOnFinish();
        }

        public R getResult() {
            return result;
        }
    }

    private class JobExecutor implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Job<?, ?> job;
                synchronized (jobQueue) {
                    while (jobQueue.isEmpty()) {
                        try {
                            jobQueue.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                    job = jobQueue.poll();
                    if (!jobQueue.isEmpty()) {
                        jobQueue.notify();
                    }
                }
                job.run();
            }
        }
    }
}

