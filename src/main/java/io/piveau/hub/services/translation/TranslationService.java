package io.piveau.hub.services.translation;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.dcatap.TripleStore;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

@ProxyGen
public interface TranslationService {
    String SERVICE_ADDRESS = "io.piveau.hub.translationservice.queue";

    // Required static methods
    // based on the documentation (https://github.com/vert-x3/vertx-service-proxy)
    static TranslationService create(Vertx vertx, WebClient client, JsonObject config, TripleStore tripleStore,
                                     Handler<AsyncResult<TranslationService>> readyHandler) {
        return new TranslationServiceImpl(vertx, client, config, tripleStore, readyHandler);
    }

    static TranslationService createProxy(Vertx vertx, String address) {
        return new TranslationServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    TranslationService initializeTranslationProcess(DatasetHelper helper,DatasetHelper oldHelper, Handler<AsyncResult<DatasetHelper>> asyncHandler);

    @Fluent
    TranslationService receiveTranslation(JsonObject translation, Handler<AsyncResult<JsonObject>> asyncHandler);
}
