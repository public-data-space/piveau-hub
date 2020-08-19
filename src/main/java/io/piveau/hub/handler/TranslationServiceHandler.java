package io.piveau.hub.handler;

import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class TranslationServiceHandler {



    private TranslationService translationService;

    public TranslationServiceHandler(Vertx vertx, String address) {
        PiveauLoggerFactory.getLogger(getClass()).debug("Translation Service Handler initiated.");
        this.translationService = TranslationService.createProxy(vertx, address);
    }

    public void handlePostTranslationRequest(RoutingContext context) {
        JsonObject translation = context.getBodyAsJson();
        this.translationService.initializeTranslationProcess(null,null, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        });
    }

    public void handlePostTranslation(RoutingContext context) {
        JsonObject translation = context.getBodyAsJson();
        PiveauLoggerFactory.getLogger(getClass()).debug(translation.toString());
        this.translationService.receiveTranslation(translation, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        context.response().setStatusCode(200).end();
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end();
                        break;
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        });
    }
}
