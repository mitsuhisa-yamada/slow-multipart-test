public class TimeRecorder {
    public void record(long totalNs, long sleepNs) {
        count++;

        sum += totalNs;
        min = (min < 0) ? totalNs : Math.min(min, totalNs);
        max = Math.max(max, totalNs);

        long nonSleep = totalNs - sleepNs;
        nonSleepSum += nonSleep;
        nonSleepMin = (nonSleepMin < 0) ? nonSleep : Math.min(nonSleepMin, nonSleep);
        nonSleepMax = Math.max(nonSleepMax, nonSleep);
    }

    public int getCount() {
        return count;
    }

    public long getAvg() {
        return (count > 0) ? sum / count : 0;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public long getNonSleepAvg() {
        return (count > 0) ? nonSleepSum / count : 0;
    }

    public long getNonSleepMin() {
        return nonSleepMin;
    }

    public long getNonSleepMax() {
        return nonSleepMax;
    }

    private int count = 0;
    private long sum = 0;
    private long min = -1;
    private long max = 0;
    private long nonSleepSum = 0;
    private long nonSleepMin = -1;
    private long nonSleepMax = 0;
}
