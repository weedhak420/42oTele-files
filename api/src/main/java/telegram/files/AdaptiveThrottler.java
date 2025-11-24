package telegram.files;

public class AdaptiveThrottler {

    private static final int MIN_CONCURRENCY = 2;

    private static final int MAX_CONCURRENCY = 20;

    private volatile int currentLimit = MIN_CONCURRENCY;

    public int calculateLimit(long avgSpeedBytesPerSecond, int configuredLimit) {
        int boundedConfiguredLimit = boundLimit(configuredLimit);

        if (avgSpeedBytesPerSecond > 5L * 1024 * 1024) {
            currentLimit = Math.min(MAX_CONCURRENCY, Math.min(boundedConfiguredLimit, currentLimit + 2));
        } else if (avgSpeedBytesPerSecond < 500L * 1024) {
            currentLimit = Math.max(MIN_CONCURRENCY, Math.min(boundedConfiguredLimit, currentLimit - 1));
        }

        currentLimit = Math.min(boundedConfiguredLimit, boundLimit(currentLimit));
        return currentLimit;
    }

    public int getCurrentLimit(int configuredLimit) {
        int boundedConfiguredLimit = boundLimit(configuredLimit);
        currentLimit = Math.min(boundedConfiguredLimit, boundLimit(currentLimit));
        return currentLimit;
    }

    public void reset(int configuredLimit) {
        currentLimit = boundLimit(configuredLimit);
    }

    private int boundLimit(int limit) {
        return Math.max(MIN_CONCURRENCY, Math.min(MAX_CONCURRENCY, limit));
    }
}
