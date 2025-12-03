# Build and Verification Checklist

Phase 6 requires validating the optimized Telegram downloader features and ensuring the build artifacts are produced. Use the following sequence:

1. **Clean build**
   ```bash
   ./gradlew clean build --no-daemon
   ```
2. **Shadow JAR**
   ```bash
   ./gradlew shadowJar --no-daemon
   ```
3. **Feature checks**
   - Adaptive throttling and priority-based downloading still trigger for active queues.
   - Retry and checkpoint handling continue to recover failed downloads and resume history scans.
   - Asynchronous file operations are invoked for file clean-up.
   - Configuration changes hot-reload through `ConfigurationService`.

> If dependencies cannot be downloaded from Maven Central (HTTP 403), rerun after network access is available; the commands above must succeed to complete verification.
