public class TimeRecorder {
    public void record(long totalNs, long clientSleepMs, long serverSleepMs) {
        count++;

        totalSum += totalNs;
        totalMin = (totalMin < 0) ? totalNs : Math.min(totalMin, totalNs);
        totalMax = Math.max(totalMax, totalNs);

        long active = totalNs - (clientSleepMs + serverSleepMs) * 1000000;
        activeSum += active;
        activeMin = (activeMin < 0) ? active : Math.min(activeMin, active);
        activeMax = Math.max(activeMax, active);
    }

    public int getCount() {
        return count;
    }

    public long getTotalAvg() {
        return (count > 0) ? totalSum / count : 0;
    }

    public long getTotalMin() {
        return totalMin;
    }

    public long getTotalMax() {
        return totalMax;
    }

    public long getActiveAvg() {
        return (count > 0) ? activeSum / count : 0;
    }

    public long getActiveMin() {
        return activeMin;
    }

    public long getActiveMax() {
        return activeMax;
    }

    private int count = 0;
    private long totalSum = 0;
    private long totalMin = -1;
    private long totalMax = 0;
    private long activeSum = 0;
    private long activeMin = -1;
    private long activeMax = 0;
}
