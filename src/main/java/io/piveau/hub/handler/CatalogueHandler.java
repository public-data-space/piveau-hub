package io.piveau.hub.handler;

import io.piveau.hub.services.catalogues.CataloguesService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.RTPTokenHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;

public class CatalogueHandler {


    private CataloguesService cataloguesService;
    private WebClient client;

    public CatalogueHandler(Vertx vertx, String address) {
        this.cataloguesService = CataloguesService.createProxy(vertx, address);
        this.client = WebClient.create(vertx);
    }

    public void handlePostCatalogue(RoutingContext context) {
        throw new UnsupportedOperationException();
    }

    public void handlePutCatalogue(RoutingContext context) {

        PiveauLoggerFactory.getLogger(getClass()).info("Handle Catalogue");
        String id = context.pathParam("id");

        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(id,getClass());
        String hash = context.queryParam("hash").size() > 0 ? context.queryParam("hash").get(0) : null;
        log.info("hash: {}", hash);
        String contentType = context.parsedHeaders().contentType().rawValue();
        log.info("received content type: {}", contentType);

        // Content-Type can look like: multipart/form-data; charset=utf-8; boundary=something, (see: https://tools.ietf.org/html/rfc7231#section-3.1.1.1) we need the first part
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];

        if (!Constants.ALLOWED_CONTENT_TYPES.contains(contentType)) {
            context.response().setStatusCode(400).end("Content-Type header should have one of the following values: " + String.join(", ", Constants.ALLOWED_CONTENT_TYPES));
            return;
        }

        String catalogue = context.getBodyAsString();
        RTPTokenHelper rtpTokenHelper = new RTPTokenHelper(context);

        cataloguesService.putCatalogue(id, catalogue, contentType, hash, ar -> {
            if (ar.succeeded()) {
                JsonObject status = ar.result();

                switch (status.getString("status")) {
                    case "created":
                        String location = status.getString("location", "");
                        if (!location.isEmpty()) {
                            context.response().putHeader("location", location);
                        }
                        rtpTokenHelper.createKeycloakResource(client, context.data().get("owner").toString(), context.data().get("catalogue").toString());
                        context.response().setStatusCode(201).end();
                        break;
                    case "updated":
                        context.response().setStatusCode(200).end();
                        break;
                    default:
                        // should not happen, succeeded path should only respond with 2xx codes
                        context.response().setStatusCode(400).end();
                }
            } else {
                switch (ar.cause().getMessage()) {
                    case "skipped":
                        context.response().setStatusCode(304).end("Dataset is up to date");
                        break;
                    default:
                        log.error("Handling Error", ar.cause());
                        context.response().setStatusCode(400).end(ar.cause().getMessage());
                }
            }
        });
    }

    public void handleListCatalogues(RoutingContext context) {

        String accept = context.getAcceptableContentType();

        Integer limit = context.queryParam("limit").size() > 0 ? Integer.parseInt(context.queryParam("limit").get(0)) : 100;
        Integer offset = context.queryParam("offset").size() > 0 ? Integer.parseInt(context.queryParam("offset").get(0)) : 0;

        cataloguesService.listCatalogues(accept, limit, offset, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                if ("success".equals(result.getString("status"))) {
                    context.response().putHeader("Content-Type", result.getString("contentType")).end(result.getString("content"));
                } else {
                    context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        });

    }


    public void handleGetCatalogue(RoutingContext context) {
        String id = context.pathParam("id");
        String acceptType = context.getAcceptableContentType();
        cataloguesService.getCatalogue(id, acceptType, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        context.response().putHeader("Content-Type", result.getString("contentType")).end(result.getString("content"));
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


    public void handleDeleteCatalogue(RoutingContext context) {
        String id = context.pathParam("id");
        cataloguesService.deleteCatalogue(id, ar -> {
            if (ar.succeeded()) {
                RTPTokenHelper helper = new RTPTokenHelper(context);
                helper.deleteKeycloakResource(client);
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(404).end();
            }
        });
    }
}
