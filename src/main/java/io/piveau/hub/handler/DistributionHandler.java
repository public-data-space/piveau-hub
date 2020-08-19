package io.piveau.hub.handler;

import io.piveau.hub.services.distributions.DistributionsService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class DistributionHandler {

    private final DistributionsService distributionsService;

    public DistributionHandler(Vertx vertx, String address) {
        distributionsService = DistributionsService.createProxy(vertx, address);
    }

    public void handleGetDistribution(RoutingContext context) {
        String id = context.pathParam("id");
        String acceptType = context.getAcceptableContentType();

        //not Empty and not false -> true
        boolean useIdentifier = !context.queryParam("useIdentifier").isEmpty() && !context.queryParam("useIdentifier").get(0).equals("false");

        if (useIdentifier) {
            distributionsService.getDistributionByIdentifier(id, acceptType, getGetResponseHandler(context, acceptType));
        } else {
            distributionsService.getDistribution(id, acceptType, getGetResponseHandler(context, acceptType));
        }
    }

    private Handler<AsyncResult<JsonObject>> getGetResponseHandler(RoutingContext context, String acceptType) {
        return ar -> {
            if (ar.succeeded()) {
                String status = ar.result().getString("status");
                switch (status) {
                    case "success":
                        context.response().putHeader("Content-Type", acceptType).end(ar.result().getString("content"));
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end();
                        break;
                    default:
                        context.response().setStatusCode(500).end(status);
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        };
    }

    public void handlePutDistribution(RoutingContext context) {


        String contentType = context.parsedHeaders().contentType().rawValue();


        // Content-Type can look like: multipart/form-data; charset=utf-8; boundary=something, (see: https://tools.ietf.org/html/rfc7231#section-3.1.1.1) we need the first part
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];

        if (!Constants.ALLOWED_CONTENT_TYPES.contains(contentType)) {
            context.response().setStatusCode(400).end("Content-Type header should have one of the following values: " + String.join(", ", Constants.ALLOWED_CONTENT_TYPES));

            return;
        }
        String id = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);
        String distribution = context.getBodyAsString();
        boolean useIdentifier = !context.queryParam("useIdentifier").isEmpty() && !context.queryParam("useIdentifier").get(0).equals("false");


        if (useIdentifier) {
            distributionsService.putDistributionWithIdentifier(distribution, id, contentType, getPutResponseHandler(context));
        } else {
            distributionsService.putDistribution(distribution, id, contentType, getPutResponseHandler(context));
        }

    }


    private Handler<AsyncResult<JsonObject>> getPutResponseHandler(RoutingContext context) {

        return ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        context.response().setStatusCode(HttpStatus.SC_NO_CONTENT).end();
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end(result.getString("content", ""));
                        break;
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        };
    }

    public void handlePostDistribution(RoutingContext context) {


        String datasetId = context.queryParam("dataset").get(0);
        String catalogue = !context.queryParam("catalogue").isEmpty() ? context.queryParam("catalogue").get(0) : null;

        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogue, getClass());
        boolean normalized = !context.queryParam("useNormalizedID").isEmpty() && !context.queryParam("useNormalizedID").get(0).equals("false");

        String contentType = context.parsedHeaders().contentType().rawValue();
        log.info("received content type: {}", contentType);

        // Content-Type can look like: multipart/form-data; charset=utf-8; boundary=something, (see: https://tools.ietf.org/html/rfc7231#section-3.1.1.1) we need the first part
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];

        if (!Constants.ALLOWED_CONTENT_TYPES.contains(contentType)) {
            context.response().setStatusCode(400).end("Content-Type header should have one of the following values: " + String.join(", ", Constants.ALLOWED_CONTENT_TYPES));
            log.info("returned 400");
            return;
        }

        if (!normalized && catalogue == null) {
            context.response().setStatusCode(400).end("Using one of these parameters is required: catalogue, useNormalizedID");
            log.info("returned 400");
            return;
        }
        log.info("posting");
        String distribution = context.getBodyAsString();
        distributionsService.postDistribution(distribution, datasetId, contentType, catalogue, getPostHandler(context));
        // Handler<AsyncResult<JsonObject>> har = getPostHandler(context);
    }

    public void handleDeleteDistribution(RoutingContext context) {

        String id = context.pathParam("id");

        distributionsService.deleteDistribution(id, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        context.response().setStatusCode(200).end(result.getString("content", ""));
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end(result.getString("content", ""));
                        break;
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage() != null ? ar.cause().getMessage() : "Internal Server Error");
            }
        });
    }

    private Handler<AsyncResult<JsonObject>> getPostHandler(RoutingContext context) {

        return ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        String location = result.getString(HttpHeaders.LOCATION, "");
                        if (!location.isEmpty()) {
                            context.response().putHeader(HttpHeaders.LOCATION, location);
                        }
                        context.response().setStatusCode(201).end();
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end();
                        break;
                    case "already exists":
                        context.response().setStatusCode(409).end(result.getString("content", ""));
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        };
    }

}
