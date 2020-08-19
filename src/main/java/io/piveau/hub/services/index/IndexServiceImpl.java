package io.piveau.hub.services.index;

import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;

import java.util.ArrayList;
import java.util.List;

public class IndexServiceImpl implements IndexService {

    private WebClient client;
    private CircuitBreaker breaker;

    private Integer port;
    private String url;
    private String apiKey;


    IndexServiceImpl(WebClient client, CircuitBreaker breaker, JsonObject config, Handler<AsyncResult<IndexService>> readyHandler) {
        this.client = client;
        this.breaker = breaker;

        this.port = config.getInteger("port", 8080);
        this.url = config.getString("url", "localhost");
        this.apiKey = config.getString("api_key", "");

        readyHandler.handle(Future.succeededFuture(this));
    }

    /**
     * Sends a dataset to the search service
     *
     * @param dataset
     */
    @Override
    public IndexService addDatasetWithoutCB(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler) {

        JsonObject payload = dataset;

        JsonArray putArray = new JsonArray();
        JsonArray postArray = new JsonArray();

        List<Future> existList = new ArrayList<>();

        payload.getJsonArray("datasets").forEach(item -> {
            JsonObject obj = (JsonObject) item;
            PiveauLogger LOGGER = PiveauLoggerFactory.getDatasetLogger(obj.getString("id"),getClass());
            Promise existPromise = Promise.promise();
            existList.add(existPromise.future());
            entityExists(obj.getString("id"), "datasets").onComplete(existHandler -> {
                if (existHandler.succeeded()) {
                    if (existHandler.result()) {
                        putArray.add(obj);
                        existPromise.complete();
                    } else {
                        postArray.add(obj);
                        existPromise.complete();
                    }
                } else {
                    LOGGER.error("Unable to check if dataset exits" + existHandler.cause());
                    existPromise.complete();
                }
            });
        });

        // LOGGER.info(payload.encodePrettily());
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

        CompositeFuture.all(existList).onComplete(ar -> {
            if (ar.succeeded()) {

                JsonObject putDatasets = new JsonObject();
                JsonObject postDatasets = new JsonObject();

                putDatasets.put("datasets", putArray);
                postDatasets.put("datasets", postArray);


                if (putArray.size() != 0) {
                    HttpRequest<Buffer> putRequest = client.put(this.port, this.url, "/datasets")
                            .putHeader("Authorization", this.apiKey)
                            .putHeader("Content-Type", "application/json");

                    breaker.execute(promise -> putRequest.sendJsonObject(putDatasets, ar2 -> {
                        if (ar2.succeeded()) {
                            promise.complete();
                        } else {
                            LOGGER.error("Unable to send dataset to Search Service. ", ar2.cause());
                            promise.fail(ar2.cause());
                        }
                    })).onComplete(ar2 -> {
                        if (ar2.succeeded()) {
                            handler.handle(Future.succeededFuture());
                        } else {
                            LOGGER.error("Unable to send dataset to Search Service");
                            handler.handle(Future.failedFuture(ar2.cause()));
                        }
                    });

                }

                if (postArray.size() != 0) {
                    HttpRequest<Buffer> postRequest = client.post(this.port, this.url, "/datasets")
                            .putHeader("Authorization", this.apiKey)
                            .putHeader("Content-Type", "application/json");

                    breaker.execute(promise -> postRequest.sendJsonObject(postDatasets, ar2 -> {
                        if (ar2.succeeded()) {
                            promise.complete();
                        } else {
                            LOGGER.error(ar2.cause().getMessage());
                            promise.fail(ar2.cause());
                        }
                    })).onComplete(ar2 -> {
                        if (ar2.succeeded()) {
                            handler.handle(Future.succeededFuture());
                        } else {
                            LOGGER.error("Unable to send dataset to Search Service");
                            handler.handle(Future.failedFuture(ar2.cause()));
                        }
                    });
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }

        });
        return this;
    }


    /**
     * Sends a dataset to the search service
     *
     * @param dataset
     */
    @Override
    public IndexService addDataset(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler) {
        JsonObject payload = dataset;

        // LOGGER.info(payload.encodePrettily());

        Future<Boolean> datasetExists = entityExists(payload.getString("id"), "datasets");

        datasetExists.onComplete(ar -> {
            if (ar.succeeded()) {
                if (ar.result()) {
                    HttpRequest<Buffer> request = client.put(this.port, this.url, "/datasets/" + payload.getString("id"))
                            .putHeader("Authorization", this.apiKey)
                            .putHeader("Content-Type", "application/json");

                    breaker.execute(promise -> request.sendJsonObject(payload, ar2 -> {
                        if (ar2.succeeded()) {
                            if (ar2.result().statusCode() == 200) {
                                promise.complete();
                            } else {
                                promise.fail(ar2.result().bodyAsString());
                            }
                        } else {
                            promise.fail(ar2.cause());
                        }
                    })).onComplete(ar2 -> {
                        if (ar2.succeeded()) {
                            handler.handle(Future.succeededFuture());
                        } else {
                            handler.handle(Future.failedFuture(ar2.cause()));
                        }
                    });
                } else {
                    HttpRequest<Buffer> request = client.post(this.port, this.url, "/datasets")
                            .putHeader("Authorization", this.apiKey)
                            .putHeader("Content-Type", "application/json");

                    breaker.execute(promise -> request.sendJsonObject(payload, ar2 -> {
                        if (ar2.succeeded()) {
                            if (ar2.result().statusCode() == 201) {
                                promise.complete();
                            } else {
                                promise.fail(ar2.result().bodyAsString());
                            }
                        } else {
                            promise.fail(ar2.cause());
                        }
                    })).onComplete(ar2 -> {
                        if (ar2.succeeded()) {
                            handler.handle(Future.succeededFuture());
                        } else {
                            handler.handle(Future.failedFuture(ar2.cause()));
                        }
                    });
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }

        });
        //handler.handle(Future.succeededFuture());
        return this;
    }

    @Override
    public IndexService addDatasetPut(JsonObject dataset, Handler<AsyncResult<JsonObject>> handler) {

        if (dataset.isEmpty()) {
            handler.handle(Future.failedFuture("Empty index object"));
            return this;
        }

        HttpRequest<Buffer> request = client.put(this.port, this.url, "/datasets/" + dataset.getString("id"))
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .expect(ResponsePredicate.SC_SUCCESS);

        request.addQueryParam("synchronous", "true");

        request.sendJsonObject(dataset, ar-> {
            if (ar.succeeded()) {
                JsonObject result = ar.result().bodyAsJsonObject();
                if (result.getBoolean("success")) {
                    handler.handle(Future.succeededFuture());
                } else {
                    handler.handle(Future.failedFuture(result.getJsonObject("result").encodePrettily()));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public IndexService deleteDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getDatasetLogger(datasetId,getClass());

        HttpRequest<Buffer> request = client.delete(this.port, this.url, "/datasets/" + datasetId)
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .expect(ResponsePredicate.SC_SUCCESS);

        request.send(ar -> {
            if (ar.succeeded()) {
                LOGGER.debug("Successfully deleted dataset from Search Service with id " + datasetId);
                handler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Unable to delete dataset from Search Service");
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public IndexService listAllDatasets(int pageLimit, int currentPage, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());
        HttpRequest<Buffer> request = client.get(this.port, this.url, "/search")
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .setQueryParam("filter", "dataset")
                .setQueryParam("page", Integer.toString(currentPage))
                .setQueryParam("limit", Integer.toString(pageLimit))
                .expect(ResponsePredicate.SC_OK);

        breaker.execute(promise -> request.send(ar -> {
            if (ar.succeeded()) {
                JsonArray allDatasets = ar.result().bodyAsJsonObject().getJsonObject("result").getJsonArray("results");
                JsonArray resultingList = new JsonArray();
                for (Object dataset : allDatasets) {
                    JsonObject data = (JsonObject) dataset;
                    resultingList.add(data.getString("id"));
                }
                promise.complete(new JsonObject().put("listDatasets", resultingList));
            } else {
                LOGGER.error(ar.cause().getMessage());
                promise.fail(ar.cause());
            }
        })).onComplete(ar -> {
            if (ar.succeeded()) {
                //LOGGER.info("Successfully list all datasets from Search Service" + ar.result().toString());
                handler.handle(Future.succeededFuture(new JsonObject(ar.result().toString())));
            } else {
                LOGGER.error("Unable to list all datasets from Search Service" + ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public IndexService listAllCIds(int pageLimit, int currentPage, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());
        HttpRequest<Buffer> request = client.get(this.port, this.url, "/search")
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .setQueryParam("filter", "catalogue")
                .setQueryParam("page", Integer.toString(currentPage))
                .setQueryParam("limit", Integer.toString(pageLimit))
                .expect(ResponsePredicate.SC_OK);

        breaker.execute(promise -> request.send(ar -> {
            if (ar.succeeded()) {
                JsonArray allDatasets = ar.result().bodyAsJsonObject().getJsonObject("result").getJsonArray("results");
                JsonArray resultingList = new JsonArray();
                for (Object dataset : allDatasets) {
                    JsonObject data = (JsonObject) dataset;
                    resultingList.add(data.getString("id"));
                }
                promise.complete(new JsonObject().put("listCatalogues", resultingList));
            } else {
                LOGGER.error(ar.cause().getMessage());
                promise.fail(ar.cause());
            }
        })).onComplete(ar -> {
            if (ar.succeeded()) {
                //LOGGER.info("Successfully list all datasets from Search Service" + ar.result().toString());
                handler.handle(Future.succeededFuture(new JsonObject(ar.result().toString())));
            } else {
                LOGGER.error("Unable to list all catalogues from Search Service" + ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public IndexService deleteCatalog(String catalogId, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getCatalogueLogger(catalogId,getClass());
        HttpRequest<Buffer> request = client.delete(this.port, this.url, "/catalogues/" + catalogId)
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .expect(ResponsePredicate.SC_OK);

        breaker.execute(promise -> request.send(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                LOGGER.error(ar.cause().getMessage());
                promise.fail(ar.cause());
            }
        })).onComplete(ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Successfully deleted catalog from Search Service");
                handler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Unable to delete catalog from Search Service");
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }


    /**
     * Sends a catalog to the search service
     *
     * @param catalog
     */
    @Override
    public IndexService addCatalog(JsonObject catalog, Handler<AsyncResult<JsonObject>> handler) {
        JsonObject payload = catalog;
        PiveauLogger LOGGER = PiveauLoggerFactory.getCatalogueLogger(payload.getString("id"),getClass());

        //Future<Boolean> catalogExists = entityExists(payload.getString("id"), "catalogues");

        HttpRequest<Buffer> request = client.put(this.port, this.url, "/catalogues/" + payload.getString("id"))
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json")
                .expect(ResponsePredicate.SC_SUCCESS);

        request.sendJsonObject(payload, ar -> {
            if (ar.succeeded()) {
                LOGGER.debug("Successfully sent catalog to Search Service (PUT)");
                handler.handle(Future.succeededFuture());
            } else {
                LOGGER.error("Unable to send catalog to Search Service");
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

//
//        catalogExists.setHandler(ar -> {
//            if (ar.succeeded()) {
//                if (ar.result()) {
//                    HttpRequest<Buffer> request = client.put(this.port, this.url, "/catalogues/" + payload.getString("id"))
//                            .putHeader("Authorization", this.apiKey)
//                            .putHeader("Content-Type", "application/json")
//                            .expect(ResponsePredicate.SC_OK);
//
//                    breaker.execute(fut2 -> request.sendJsonObject(payload, ar2 -> {
//                        if (ar2.succeeded()) {
//                            fut2.complete();
//                        } else {
//                            LOGGER.error(ar2.cause().getMessage());
//                            fut2.fail(ar2.cause());
//                        }
//                    })).setHandler(ar2 -> {
//                        if (ar2.succeeded()) {
//                            LOGGER.info("Successfully sent catalog to Search Service (PUT)");
//                            handler.handle(Future.succeededFuture());
//                        } else {
//                            LOGGER.error("Unable to send catalog to Search Service");
//                            handler.handle(Future.failedFuture(ar2.cause()));
//                        }
//                    });
//                } else {
//                    HttpRequest<Buffer> request = client.post(this.port, this.url, "/catalogues")
//                            .putHeader("Authorization", this.apiKey)
//                            .putHeader("Content-Type", "application/json")
//                            .expect(ResponsePredicate.SC_CREATED);
//
//                    breaker.execute(fut2 -> request.sendJsonObject(payload, ar2 -> {
//                        if (ar2.succeeded()) {
//                            fut2.complete();
//                        } else {
//                            LOGGER.error(ar2.cause().getMessage());
//                            fut2.fail(ar2.cause());
//                        }
//                    })).setHandler(ar2 -> {
//                        if (ar2.succeeded()) {
//                            LOGGER.info("Successfully sent catalog to Search Service (POST)");
//                            handler.handle(Future.succeededFuture());
//                        } else {
//                            LOGGER.error("Unable to send catalog to Search Service");
//                            handler.handle(Future.failedFuture(ar2.cause()));
//                        }
//                    });
//                }
//            } else {
//                handler.handle(Future.failedFuture(ar.cause()));
//            }
//
//        });

        return this;
    }


    private Future<Boolean> entityExists(String id, String type) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(type.equals("dataset")?id:"",type.equals("catalogue")?id:"",getClass());

        Promise<Boolean> result = Promise.promise();
        HttpRequest<Buffer> getRequest = client.get(this.port, this.url, "/" + type + "/" + id)
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json");
        breaker.execute(promise -> getRequest.send(ar -> {
            if (ar.succeeded()) {
                if (ar.result().statusCode() == 200) {
                    //LOGGER.info(type + " " + id + " already exists");
                    result.complete(true);
                    promise.complete();
                } else if (ar.result().statusCode() == 404) {
                    LOGGER.info(type + " " + id + " does not exist");
                    result.complete(false);
                    promise.complete();
                } else {
                    promise.fail("Unsupported Status Code: " + ar.result().statusCode());
                }
            } else {
                promise.fail(ar.cause());
            }
        })).onComplete(ar -> {
            if (ar.succeeded()) {
                //LOGGER.info("Successfully communicated with Search Service");
            } else {
                result.fail("Unable to call search service " + ar.cause().getMessage());
            }
        });

        return result.future();
    }


}
