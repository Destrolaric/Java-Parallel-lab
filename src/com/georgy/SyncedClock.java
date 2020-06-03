package com.georgy;

class SyncedClock {
    final int resultedValue;
    volatile int value = 0;

    public SyncedClock(int finishValue) {
        this.resultedValue = finishValue;
    }

    public synchronized void incAndNotifyOnFinish() {
        if (++value == resultedValue) {
            this.notify();
        }
    }

    public synchronized void waitUntilFinish() throws InterruptedException {
        while (resultedValue != value) {
            this.wait();
        }
    }
}
