# Migration Guide

This document highlights notable behavioural changes introduced during the refactor work so far and provides guidance for teams upgrading existing deployments.

## Repository Segmentation
- `FileRepositoryFacade` now exposes query, command, statistics, and search adapters. Existing callers can keep using `FileRepositoryImpl` directly, but new code should consume the facade to reduce coupling.
- Repository methods remain backward compatible; no schema changes are required.

## Automation Rule Priority
- `SettingAutoRecords.Automation` now includes a `priority` field. Lower values are treated as higher priority when multiple automation entries match the same message or chat.
- When migrating existing automation definitions, set `priority` to `0` to keep the previous behaviour.

## Caching Defaults
- Caffeine caches (3 minute TTL) now back settings, chat lists, statistics, and session data. When debugging, invalidate entries if stale data is suspected.

## Testing Entrypoints
- New unit tests cover cache singleton behaviour, automation rule selection, download queue handling, and repository facade delegation. Use `./gradlew test` to validate local changes and produce a JaCoCo coverage report (`api/build/reports/jacoco/test/xml`).

## Download Coordination Utilities
- Auto-download orchestration now relies on `DownloadStateManager`, `DownloadScheduler`, `AutomationRuleEngine`, and `DownloadEngine`. Extend these components rather than adding logic directly to `AutoDownloadVerticle` to keep responsibilities focused.
