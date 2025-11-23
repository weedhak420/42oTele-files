package telegram.files.handler;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.web.RoutingContext;

/**
 * Dedicated handler for simple health and metrics endpoints.
 * This class encapsulates the Vert.x health check registration and exposes
 * small request handlers that can be registered by the HTTP verticle without
 * duplicating boilerplate setup code.
 */
public class HealthCheckHandler {

    private final io.vertx.ext.web.healthchecks.HealthCheckHandler healthCheckHandler;

    public HealthCheckHandler(Vertx vertx) {
        HealthChecks healthChecks = HealthChecks.create(vertx);
        // Basic liveness registration mirrors the previous inline implementation.
        healthChecks.register("http-server", Promise::complete);
        this.healthCheckHandler = io.vertx.ext.web.healthchecks.HealthCheckHandler.createWithHealthChecks(healthChecks);
    }

    /**
     * Responds with a simple hello payload to verify the server is reachable.
     */
    public void handleHello(RoutingContext context) {
        context.response().end("Hello World!");
    }

    /**
     * Delegates to Vert.x health checks for liveness information.
     */
    public void handleHealth(RoutingContext context) {
        healthCheckHandler.handle(context);
    }

    /**
     * Simple placeholder metrics endpoint that can be expanded later.
     */
    public void handleMetrics(RoutingContext context) {
        JsonObject metrics = new JsonObject().put("status", "ok");
        context.response()
                .putHeader("Content-Type", "application/json")
                .end(metrics.encode());
    }

    /**
     * Exposes the current application version in a JSON response.
     */
    public void handleVersion(RoutingContext context, String version) {
        context.json(new JsonObject().put("version", version));
    }
}
