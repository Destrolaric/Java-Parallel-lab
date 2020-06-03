package com.georgy;

import info.kgeorgiy.java.advanced.concurrent.ScalarIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IterativeParallelism implements ScalarIP {
    ParallelMapper mapper;

    public IterativeParallelism() {}

    public IterativeParallelism(ParallelMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator)
            throws InterruptedException {

        Function<List<? extends T>, T> function = maxList -> maxList.stream().max(comparator).get();
        return function.apply(runThreads(threads, values, function));
    }

    static class WorkList<T, R> implements Runnable {
        private final Function<List<? extends T>, R> function;
        private final List<? extends T> list;
        private R result;

        public WorkList(Function<List<? extends T>, R> function, List<? extends T> list) {
            this.function = function;
            this.list = list;
        }

        @Override
        public void run() {
            result = function.apply(list);
        }

        public R getResult() {
            return result;
        }
    }

    private <T, R> List<R> runThreads(
            int threads, List<? extends T> values, Function<List<? extends T>, R> function)
            throws InterruptedException {
        List<List<? extends T>> jobsList = splitList(threads, values);
        if (mapper != null) {
            return mapper.map(function, jobsList);
        }
        List<WorkList<T, R>> workers =
                jobsList.stream().map(val -> new WorkList<>(function, val)).collect(Collectors.toList());
        List<Thread> threadsList = workers.stream().map(Thread::new).collect(Collectors.toList());
        threadsList.forEach(Thread::start);
        for (Thread thread : threadsList) {
            thread.join();
        }

        return workers.stream().map(WorkList::getResult).collect(Collectors.toList());
    }

    private <T> List<List<? extends T>> splitList(int threads, List<? extends T> values) {
        if (threads < 1) {
            throw new IllegalArgumentException("Wrong threads count: " + threads);
        }

        int chunksize = threads < values.size() ? values.size() / threads : 1;
        List<List<? extends T>> result = new ArrayList<>();

        for (int chunkStart = 0; chunkStart < values.size(); chunkStart += chunksize) {
            int chunkEnd = Math.min(chunkStart + chunksize, values.size());
            result.add(values.subList(chunkStart, chunkEnd));
        }
        return result;
    }

    @Override
    public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator)
            throws InterruptedException {
        return maximum(threads, values, comparator.reversed());
    }

    @Override
    public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate)
            throws InterruptedException {
        Function<List<? extends T>, Boolean> function = val -> val.stream().allMatch(predicate);
        return runThreads(threads, values, function).stream().allMatch(val -> val);
    }

    @Override
    public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate)
            throws InterruptedException {
        return !all(threads, values, predicate.negate());
    }
}
