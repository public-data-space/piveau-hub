package io.piveau.hub.services.catalogues;

import io.piveau.hub.util.TSConnector;
import io.piveau.dcatap.TripleStore;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface CataloguesService {
    String SERVICE_ADDRESS="io.piveau.hub.catalogues.queue";

    static CataloguesService create(TripleStore tripleStore, TSConnector connector, Vertx vertx, Handler<AsyncResult<CataloguesService>> readyHandler) {
        return new CataloguesServiceImpl(tripleStore, connector, vertx, readyHandler);
    }

    static CataloguesService createProxy(Vertx vertx, String address) {
        return new CataloguesServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    CataloguesService listCatalogues(String consumes, Integer limit, Integer offset, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService getCatalogue(String id, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService putCatalogue(String id, String catalogue, String contentType, String hash, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService postCatalogue(String catalogue, String contentType, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService deleteCatalogue(String id, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService existenceCheckCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler);

}
