# Architecture Overview

This document summarizes the refactoring targets and service boundaries for the Telegram Files API. It is intended to guide incremental work that breaks the current monolithic verticles and repositories into composable, testable units while preserving existing API contracts and configuration.

## HTTP Layer

- **HttpVerticle** should only create the HTTP server, configure shared middleware (sessions, CORS, body handling), and register route handlers.
- **Route handlers** own endpoint-specific logic:
  - `FileRouteHandler` manages file list, download, transfer, and deletion endpoints.
  - `TelegramRouteHandler` handles Telegram authentication, chats, status, login/logout, and ping/test-network endpoints.
  - `SettingRouteHandler` exposes settings retrieval, update, and automation configuration.
  - `StatisticRouteHandler` serves download speed, summary, and per-channel metrics.
  - `HealthCheckHandler` responds to hello/health/metrics/version probes and remains side-effect free.

## Telegram Runtime

- **TelegramVerticle** acts purely as an orchestrator that wires and coordinates specialized services:
  - `TelegramAuthService` drives QR-code and phone-based authentication plus session lifecycle management.
  - `TelegramMessageHandler` parses and processes incoming Telegram updates.
  - `TelegramFileManager` resolves metadata, manages downloads, and tracks progress for Telegram-originating files.
  - `TelegramChatService` owns chat list retrieval, chat update handling, and chat-centric helpers.
  - `TelegramEventDispatcher` routes TDLib events to the appropriate service without embedding business logic.

## Repository Layer

- The legacy `FileRepositoryImpl` is being decomposed into dedicated adapters to satisfy single-responsibility constraints:
  - `FileQueryRepository` covers reads, pagination, and lightweight search helpers.
  - `FileCommandRepository` encapsulates inserts, updates, deletes, and batch operations.
  - `FileStatisticsRepository` provides aggregation, counts, and reporting utilities.
  - `FileSearchRepository` owns advanced/full-text search and multi-criteria filters.
- `FileRepositoryFacade` (to be promoted to `FileRepository`) coordinates the segmented repositories while maintaining the existing public API surface until callers are migrated.

## Download Automation

- **AutoDownloadVerticle** should monitor events and delegate to specialized components:
  - `DownloadEngine` executes downloads, enforces concurrency limits, and implements retry/backoff.
  - `AutomationRuleEngine` evaluates prioritized rules (size/type/time/chat/keyword) via the Strategy pattern.
  - `DownloadScheduler` manages queued work, time windows, and priority ordering.
  - `DownloadStateManager` persists state, supports pause/resume, and collects per-download statistics.
- Engines must stream large files, perform disk-space checks, and throttle bandwidth when configured.

## Service Layer

- The `service` package mediates business workflows and Vert.x interactions:
  - `FileService` coordinates repository operations, transfer flows, and cache invalidation.
  - `TelegramService` orchestrates Telegram account lifecycle, authentication, and chat/message interactions.
  - `DownloadService` bridges route handlers, automation engines, and transfer operations.
  - `AutomationService` manages rule persistence and evaluation lifecycle.

## Testing Expectations

- Unit tests should target each extracted service, handler, and repository adapter with mocked dependencies.
- Integration and end-to-end tests must cover Telegram login, download workflows, automation scheduling, and transfer completion.
- Maintain method sizes under 50 lines and class sizes under 500 lines to keep testability high.
