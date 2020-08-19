package io.piveau.hub.handler;

import io.piveau.dcatap.Prefixes;
import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.HttpHeaders;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class DatasetHandler {


    private final DatasetsService datasetsService;

    public DatasetHandler(Vertx vertx, String address) {
        datasetsService = DatasetsService.createProxy(vertx, address);
    }

    public void handleGetDataset(RoutingContext context) {

        String id = context.pathParam("id");

        String catalogueId = context.queryParam("catalogue").isEmpty() ? null : context.queryParam("catalogue").get(0);

        String acceptType;
        if (id.endsWith(".jsonld")) {
            acceptType = "application/ld+json";
            id = id.substring(0, id.length() - 7);
            context.setAcceptableContentType(acceptType);
        } else if (id.endsWith(".rdf")) {
            acceptType = "application/rdf+xml";
            id = id.substring(0, id.length() - 4);
            context.setAcceptableContentType(acceptType);
        } else if (id.endsWith(".n3")) {
            acceptType = "text/n3";
            id = id.substring(0, id.length() - 3);
            context.setAcceptableContentType(acceptType);
        } else if (id.endsWith(".ttl")) {
            acceptType = "text/turtle";
            id = id.substring(0, id.length() - 4);
            context.setAcceptableContentType(acceptType);
        } else if (id.endsWith(".nt")) {
            acceptType = "text/plain";
            id = id.substring(0, id.length() - 3);
            context.setAcceptableContentType(acceptType);
        } else {
            acceptType = context.getAcceptableContentType();
        }

        boolean normalized = !context.queryParam("useNormalizedID").isEmpty() && !context.queryParam("useNormalizedID").get(0).equals("false");

        Handler<AsyncResult<JsonObject>> har = getHandler(context, acceptType);

        if (normalized) {
            datasetsService.getDatasetByNormalizedId(id, acceptType, har);
        } else {
            datasetsService.getDataset(id, catalogueId, acceptType, har);
        }
    }


    private Handler<AsyncResult<JsonObject>> getHandler(RoutingContext context, String acceptType) {
        return ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        // JUST A WORKAROUND FOR NOW TO GET BETTER STRUCTURE FOR DEBUGGING
                        if (acceptType.equals(Lang.TURTLE.getHeaderString())) {
                            String payload = result.getString("content");
                            Model resultModel = ModelFactory.createDefaultModel();
                            RDFDataMgr.read(resultModel, new ByteArrayInputStream(payload.getBytes()), Lang.TURTLE);
                            resultModel.setNsPrefixes(Prefixes.DCATAP_PREFIXES);
                            StringWriter out = new StringWriter();
                            RDFDataMgr.write(out, resultModel, RDFFormat.TURTLE);
                            context.response().putHeader("Content-Type", result.getString("contentType")).end(out.toString());
                        } else {
                            context.response().putHeader("Content-Type", result.getString("contentType")).end(result.getString("content"));
                        }
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
        };
    }

    public void handleGetRecord(RoutingContext context) {
        String datasetId = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);
        String catalogueId = context.queryParam("catalogue").get(0);
        String acceptType = context.getAcceptableContentType();
        datasetsService.getRecord(datasetId, catalogueId, acceptType, ar -> {
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


    public void handlePutDataset(RoutingContext context) {
        String idParam = context.pathParam("id");
        if (idParam == null) {
            idParam = context.queryParam("id").get(0);
        }
        final String id = idParam;

        String catalogueId = context.queryParam("catalogue").get(0);
        Boolean dataUpload = !context.queryParam("data").isEmpty() && context.queryParam("data").get(0).equals("true");

        String hash = context.queryParam("hash").isEmpty() ? null : context.queryParam("hash").get(0);
        String contentType = context.parsedHeaders().contentType().value();

        String content = context.getBodyAsString();
        datasetsService.putDataset(id, content, contentType, catalogueId, hash, dataUpload, ar -> {
            if (ar.succeeded()) {
                JsonObject status = ar.result();
                switch (status.getString("status")) {
                    case "created":
                        if (dataUpload) {
                            String resultDataset = status.getString("dataset");
                            datasetsService.getDataUploadInformation(id, catalogueId, resultDataset, du -> {
                                if (du.succeeded()) {
                                    context.response()
                                            .setStatusCode(201)
                                            .putHeader(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION, ""))
                                            .putHeader("Content-Type", "application/json")
                                            .end(du.result().toString());
                                } else {
                                    context.response().
                                            putHeader("Content-Type", "application/json")
                                            .setStatusCode(500)
                                            .end(new JsonObject().put("status", "error").put("message", du.cause().getMessage()).toString());
                                }
                            });
                        } else {
                            context.response().setStatusCode(201).putHeader(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION, "")).end();
                        }
                        break;
                    case "updated":
                        context.response().setStatusCode(200).end();
                        break;
                    default:
                        // should not happen, succeeded path should only respond with 2xx codes
                        context.response().setStatusCode(400).end();
                }
            } else {
                if (ar.cause().getMessage() != null) {
                    switch (ar.cause().getMessage()) {
                        case "no catalogue":
                            context.response().setStatusCode(400).end("No catalogue with given id found.");
                            break;
                        case "skipped":
                            context.response().setStatusCode(304).end("Dataset is up to date");
                            break;
                        default:
                            context.response().setStatusCode(400).end(ar.cause().getMessage());
                    }
                } else {
                    context.response().setStatusCode(500).end();
                }
            }
        });
    }

    public void handleDeleteDataset(RoutingContext context) {
        String id = context.pathParam("id");
        if (id == null) {
            id = context.queryParam("id").get(0);
        }
        String catalogueId = context.queryParam("catalogue").get(0);
        datasetsService.deleteDataset(id, catalogueId, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
            }
        });
    }

    public void handlePostDataset(RoutingContext context) {
        // Set up Logging
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        log.info("Handle Post Dataset");

        // Parse ContentType e.g multipart/form-data; charset=utf-8; boundary=something, ->  multipart/form-data
        String contentType = context.parsedHeaders().contentType().rawValue();
        log.info("received content type: {}", contentType);
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];
        // Check for valid ContentTypes
        if (!Constants.ALLOWED_CONTENT_TYPES.contains(contentType)) {
            context.response().setStatusCode(400).end("Content-Type header should have one of the following values: " + String.join(", ", Constants.ALLOWED_CONTENT_TYPES));
            log.info("returned 400");
            return;
        }

        // Parse Dataset from Context
        String dataset = context.getBodyAsString();
        String catalogueId = context.queryParam("catalogue").get(0);
        Boolean dataUpload = !context.queryParam("data").isEmpty() && context.queryParam("data").get(0).equals("true");


        // Handle Post Dataset
        datasetsService.postDataset(dataset, contentType, catalogueId, dataUpload, ar -> {
            // Post Dataset Succeded
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                // Succesfully Created
                if (result.getString("status").equals("created")) {
                    log.info("Created New Dataset. Respond with 201");
                    context.response().setStatusCode(201)
                            .putHeader(HttpHeaders.LOCATION, result.getString(HttpHeaders.LOCATION, ""))
                            .end(result.getString("id"));
                } else { // Should not happen
                    log.error("Post Dataset Successfull. But Result '" + ar.result().getString("status") + "' is not expected");
                    context.response().setStatusCode(500).end();
                }
            }
            // Post Dataset Failed
            else {
                log.error("Post Dataset failed.", ar.cause());
                Optional<String> message = Optional.ofNullable(ar.cause().getMessage());
                message.ifPresentOrElse(mes -> {
                            context.response().setStatusCode(400).end(mes);
                        }, () -> {
                            context.response().setStatusCode(500).end();
                        }
                );
            }
        });
    }

    public void handleListDatasets(RoutingContext context) {
        String accept = context.getAcceptableContentType();
        String catalogueId = context.queryParam("catalogue").size() > 0 ? context.queryParam("catalogue").get(0) : null;
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(catalogueId, getClass());

        Integer limit = context.queryParam("limit").size() > 0 ? Integer.parseInt(context.queryParam("limit").get(0)) : null;
        Integer offset = context.queryParam("offset").size() > 0 ? Integer.parseInt(context.queryParam("offset").get(0)) : null;

        //offset works only with limit set
        if (offset != null && limit == null) {
            context.response().setStatusCode(400).end("Offset is only allowed when limit is set");
        }

        Boolean sourceIds = !context.queryParam("sourceIds").isEmpty() && !context.queryParam("sourceIds").get(0).equals("false");
        log.info("sourceIds is not empty or false: {}", sourceIds);

        datasetsService.listDatasets(accept, catalogueId, limit, offset, sourceIds, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                if ("success".equals(result.getString("status"))) {
                    context.response().putHeader("Content-Type", result.getString("contentType")).end(result.getValue("content").toString());
                } else {
                    context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        });
    }

    public void handleIndexDataset(RoutingContext context) {
        String datasetId = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);
        String catalogueId = context.queryParam("catalogue").get(0);
        String defaultLanguage = context.queryParam("language").get(0);

        datasetsService.indexDataset(datasetId, catalogueId, defaultLanguage, ar -> {
            if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "application/json").setStatusCode(200).end();
            } else {
                context.response().
                        putHeader("Content-Type", "application/json")
                        .setStatusCode(500)
                        .end(new JsonObject().put("status", "error").put("message", ar.cause().getMessage()).toString());
            }
        });
    }

}
