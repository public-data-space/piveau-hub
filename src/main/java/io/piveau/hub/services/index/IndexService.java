package io.piveau.hub.services.index;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

@ProxyGen
public interface IndexService {
    String SERVICE_ADDRESS = "io.piveau.hub.index.queue";

    static IndexService create(WebClient client, CircuitBreaker breaker, JsonObject config, Handler<AsyncResult<IndexService>> readyHandler) {
        return new IndexServiceImpl(client, breaker, config, readyHandler);
    }

    static IndexService createProxy(Vertx vertx, String address) {
        return new IndexServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    IndexService addDatasetWithoutCB(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService addDataset(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService deleteDataset(String id, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService deleteCatalog(String id, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService addCatalog(JsonObject catalog, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService listAllDatasets(int pageLimit, int currentPage, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService listAllCIds(int pageLimit, int currentPage, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    IndexService addDatasetPut(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler);

}
