package io.piveau.hub.util;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

final public class ErrorCodeResponse {

    static public final void forbidden(RoutingContext context) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(403).end();
    }

    static public final void forbidden(RoutingContext context, String errorCause) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(403);
        if (errorCause != null) {
            response.put("status", "error");
            response.put("cause", errorCause);
            context.response().end(response.toString());
        }

        context.response().end();
    }

    static public final void internalServerError(RoutingContext context, String errorCause) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(500);
        if (errorCause != null) {
            response.put("status", "error");
            response.put("cause", errorCause);
            context.response().end(response.toString());
        }
        context.response().end();

    }

    static public final void internalServerError(RoutingContext context) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(500).end();
    }

    static public final void badRequest(RoutingContext context) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(400).end();
    }

    static public final void badRequest(RoutingContext context, String errorCause) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(400);
        if (errorCause != null) {
            response.put("status", "error");
            response.put("cause", errorCause);
            context.response().end(response.toString());
        }
        context.response().end();
    }

    static public final void unauthorized(RoutingContext context, String errorCause) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(401);
        if (errorCause != null) {
            response.put("status", "error");
            response.put("cause", errorCause);
            context.response().end(response.toString());
        }
        context.response().end();

    }

    static public final void unauthorized(RoutingContext context) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().setStatusCode(401).end();
    }

}
