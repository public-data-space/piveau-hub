package io.piveau.hub.services.metrics;

import io.piveau.pipe.PipeLauncher;
import io.piveau.dcatap.TripleStore;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface MetricsService {
    String SERVICE_ADDRESS = "io.piveau.hub.metrics.queue";

    static MetricsService create(TripleStore tripleStore, JsonObject config, PipeLauncher launcher, Handler<AsyncResult<MetricsService>> readyHandler) {
        return new MetricsServiceImpl(tripleStore, config, launcher, readyHandler);
    }

    static MetricsService createProxy(Vertx vertx, String address) {
        return new MetricsServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    MetricsService getMetric(String datasetId, String catalogueId, String contentType, Handler<AsyncResult<String>> handler);

    @Fluent
    MetricsService putMetric(String datasetUriRef, String content, String contentType, Handler<AsyncResult<String>> handler);

    @Fluent
    MetricsService putMeasurement(String data, String contentType, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    MetricsService deleteMetric(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler);

}
