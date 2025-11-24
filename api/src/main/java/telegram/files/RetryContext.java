package telegram.files;

public class RetryContext {

    private int retryCount;

    private long lastRetryTime;

    private String lastError;

    public RetryContext() {
        this.retryCount = 0;
        this.lastRetryTime = System.currentTimeMillis();
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getLastRetryTime() {
        return lastRetryTime;
    }

    public String getLastError() {
        return lastError;
    }

    public void recordFailure(Throwable e) {
        retryCount++;
        lastRetryTime = System.currentTimeMillis();
        lastError = e.getMessage();
    }
}
