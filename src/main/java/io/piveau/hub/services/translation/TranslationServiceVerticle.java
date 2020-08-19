package io.piveau.hub.services.translation;

import io.piveau.hub.util.Constants;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.TripleStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class TranslationServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject conf = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG);
        TripleStore tripleStore = new TripleStore(vertx, conf, null);
        TranslationService.create(vertx, WebClient.create(vertx), config(), tripleStore, readyHandler -> {
            if (readyHandler.succeeded()) {
                new ServiceBinder(vertx)
                        .setAddress(TranslationService.SERVICE_ADDRESS)
                        .register(TranslationService.class, readyHandler.result());
                startPromise.complete();
            } else if (readyHandler.failed()) {
                startPromise.fail(readyHandler.cause());
            }
        });
    }
}
