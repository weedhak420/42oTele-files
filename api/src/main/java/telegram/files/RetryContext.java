package telegram.files;

public class RetryContext {

    private static final long[] BACKOFF_DELAYS = {5_000L, 15_000L, 60_000L};

    private int attempts;

    private long lastAttemptTime;

    private String lastError;

    public RetryContext() {
        this.attempts = 0;
        this.lastAttemptTime = System.currentTimeMillis();
    }

    public int getAttempts() {
        return attempts;
    }

    public long getLastAttemptTime() {
        return lastAttemptTime;
    }

    public String getLastError() {
        return lastError;
    }

    public boolean shouldRetry(int maxAttempts) {
        return attempts < maxAttempts;
    }

    public long nextDelayMillis() {
        int index = Math.min(attempts, BACKOFF_DELAYS.length - 1);
        return BACKOFF_DELAYS[index];
    }

    public void recordFailure(Throwable e) {
        attempts++;
        lastAttemptTime = System.currentTimeMillis();
        lastError = e.getMessage();
    }
}
