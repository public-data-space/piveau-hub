package io.piveau.hub.handler;

import io.piveau.hub.services.metrics.MetricsService;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ServiceProxyBuilder;

public class MetricHandler {

    private MetricsService metricsService;

    public MetricHandler(Vertx vertx, String address) {
        metricsService = new ServiceProxyBuilder(vertx)
                .setAddress(address)
                .setOptions(new DeliveryOptions().setSendTimeout(60000))
                .build(MetricsService.class);
    }

    public void handleGetMetric(RoutingContext context) {
        String datasetId = context.pathParam("id");
        String catalogueId = context.queryParam("catalogue").get(0);

        String acceptType = context.getAcceptableContentType();

        metricsService.getMetric(datasetId, catalogueId, acceptType, ar -> {
            if (ar.succeeded()) {
                context.response().putHeader("Content-Type", acceptType).end(ar.result());
            } else {
                context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
            }
        });
    }

    public void handlePutMetric(RoutingContext context) {

        String datasetUriRef = context.queryParam("uriRef").get(0);

        String contentType = context.parsedHeaders().contentType().value();

        String content = context.getBodyAsString();
        metricsService.putMetric(datasetUriRef, content, contentType, ar -> {
            if (ar.succeeded()) {
                switch (ar.result()) {
                    case "created":
                        context.response().setStatusCode(201).end();
                        break;
                    case "updated":
                        context.response().setStatusCode(200).end();
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end();
                        break;
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                if (ar.cause().getMessage() != null && ar.cause().getMessage().startsWith("not found")) {
                    context.response().setStatusCode(404).setStatusMessage(ar.cause().getMessage()).end();
                } else if (ar.cause().getMessage() != null && ar.cause().getMessage().startsWith("bad request")) {
                    context.response().setStatusCode(400).setStatusMessage(ar.cause().getMessage()).end();
                } else {
                    context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
                }
            }
        });
    }

    public void handlePutMeasurement(RoutingContext context) {

        String contentType = context.parsedHeaders().contentType().value();

        String content = context.getBodyAsString();

        metricsService.putMeasurement(content, contentType, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
            }
        });
    }

    public void handleDeleteMetric(RoutingContext context) {
        String datasetId = context.pathParam("id");
        if (datasetId == null) {
            datasetId = context.queryParam("id").get(0);
        }
        String catalogueId = context.queryParam("catalogue").get(0);
        metricsService.deleteMetric(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
            }
        });
    }

}
