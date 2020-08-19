package io.piveau.hub.services.datasets;

import io.piveau.dcatap.*;
import io.piveau.dqv.PiveauMetrics;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.*;
import io.piveau.indexing.Indexing;
import io.piveau.json.ConfigHelper;
import io.piveau.pipe.PipeLauncher;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.rdf.RdfExtensionsKt;
import io.piveau.utils.*;
import io.piveau.vocabularies.vocabulary.DQV;
import io.piveau.vocabularies.vocabulary.PV;
import io.piveau.vocabularies.vocabulary.SPDX;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.http.HttpHeaders;
import org.apache.jena.query.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DatasetsServiceImpl implements DatasetsService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TripleStore tripleStore;
    private final CatalogueManager catalogueManager;
    private final DatasetManager datasetManager;
    private final MetricsManager metricsManager;

    private final TSConnector connector;
    private final DataUploadConnector dataUploadConnector;
    private final IndexService indexService;
    private final TranslationService translationService;

    private final PipeLauncher launcher;

    private final JsonObject validationConfig;
    private final JsonObject translationConfig;
    private final JsonObject indexConfig;

    DatasetsServiceImpl(TripleStore tripleStore, TSConnector connector, DataUploadConnector dataUploadConnector, JsonObject config, PipeLauncher launcher, Vertx vertx, Handler<AsyncResult<DatasetsService>> readyHandler) {
        this.launcher = launcher;
        this.connector = connector;

        this.tripleStore = tripleStore;
        catalogueManager = tripleStore.getCatalogueManager();
        datasetManager = tripleStore.getDatasetManager();
        metricsManager = tripleStore.getMetricsManager();

        this.dataUploadConnector = dataUploadConnector;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        this.translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);
        validationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_HUB_VALIDATOR);
        translationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE);
        indexConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_SERVICE);
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public DatasetsService listDatasets(String consumes, String catalogueId, Integer limit, Integer offset, Boolean sourceIds, Handler<AsyncResult<JsonObject>> handler) {
        String catalogueUriRef = catalogueId != null && !catalogueId.isEmpty() ? DCATAPUriSchema.applyFor(catalogueId).getCatalogueUriRef() : null;

        if (sourceIds) {
            connector.listDatasetSources(catalogueUriRef, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("contentType", "application/json")
                            .put("status", "success")
                            .put("content", ar.result())
                    ));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } else {
            connector.listDatasets(consumes, catalogueUriRef, limit, offset, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("contentType", consumes != null ? consumes : "application/json")
                            .put("status", "success")
                            .put("content", ar.result())
                    ));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }

        return this;
    }

    @Override
    public DatasetsService getDataset(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        datasetManager.get(datasetId, catalogueId).onSuccess(model ->
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", JenaUtils.write(model, consumes))
                ))).onFailure(throwable -> {
            if (throwable.getMessage() != null && throwable.getMessage().startsWith("not found")) {
                handler.handle(Future.succeededFuture(new JsonObject().put("status", "not found")));
            } else {
                handler.handle(Future.failedFuture(throwable));
            }
        });
        return this;
    }

    @Override
    public DatasetsService getDatasetByNormalizedId(String datasetSuffix, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        Promise<Model> graphFuture = Promise.promise();
        DCATAPUriRef dcatapSchema = DCATAPUriSchema.applyFor(datasetSuffix);
        datasetManager.getGraph(dcatapSchema.getDatasetGraphName(), graphFuture);

        graphFuture.future().onComplete(ar -> {
            if (ar.succeeded()) {

                // we could remove record here

                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", JenaUtils.write(ar.result(), consumes)
                        )));
            } else {
                try {
                    ReplyException s = (io.vertx.core.eventbus.ReplyException) ar.cause();
                    if (s.failureCode() == 404) {
                        logger.error("no dataset: {}", s.failureType());
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "not found")
                        ));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }


                } catch (ClassCastException cce) {
                    logger.error("casting is a no:", cce);
                    handler.handle(Future.failedFuture(ar.cause()));
                } catch (Exception e) {
                    logger.error("this does not work:", e);
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            }
        });
        return this;
    }


    @Override
    public DatasetsService getRecord(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        connector.getDatasetUriRefs(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                String datasetUriRef = result.getString("datasetUriRef");
                String recordUriRef = result.getString("recordUriRef");
                DCATAPUriRef uriSchema = DCATAPUriSchema.parseUriRef(datasetUriRef);
                connector.getRecord(uriSchema.getDatasetGraphName(), recordUriRef, consumes, rr -> {
                    if (rr.succeeded()) {
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "success")
                                .put("content", rr.result())
                        ));
                    } else {
                        handler.handle(Future.failedFuture(rr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService putDataset(String datasetId, String content, String contentType, String catalogueId, String hash, Boolean createAccessURLs, Handler<AsyncResult<JsonObject>> handler) {

        DatasetHelper.create(datasetId, content, contentType, hash, catalogueId, dr -> {
            if (dr.succeeded()) {
                DatasetHelper datasetHelper = dr.result();
                AtomicReference<DatasetHelper> oldDataset = new AtomicReference<>();
                Promise<JsonObject> existsPromise = Promise.promise();
                connector.catalogueExists(datasetHelper.catalogueUriRef(), existsPromise);
                existsPromise.future().compose(cat -> {
                    Promise<JsonObject> hashPromise = Promise.promise();
                    datasetHelper.sourceType(cat.getString("type"));
                    datasetHelper.sourceLang(cat.getString("lang"));
                    getHash(datasetHelper, hashPromise);
                    return hashPromise.future();
                }).compose(hr -> {
                    Promise<Void> createOrUpdatePromise = Promise.promise();
                    if (hr.getBoolean("success") && hr.getString("hash").equals(datasetHelper.hash())) {
                        logger.debug("hash equal, skipping");
                        createOrUpdatePromise.fail("skipped");
                    } else {
                        if (hr.getBoolean("success")) {
                            logger.debug("update");
                            String recordUriRef = hr.getString("recordUriRef");
                            datasetManager.getGraph(DCATAPUriSchema.parseUriRef(recordUriRef).getDatasetGraphName(), ar -> {
                                if (ar.succeeded()) {
                                    datasetHelper.update(ar.result(), recordUriRef);
                                    DatasetHelper.create(datasetId,JenaUtils.write(ar.result(), Lang.NTRIPLES), Lang.NTRIPLES.getContentType().getContentType(),hr.getString("hash"),catalogueId,res-> oldDataset.set(res.result()));
                                    createOrUpdatePromise.complete();
                                } else {
                                    createOrUpdatePromise.fail(ar.cause());
                                }
                            });
                        } else {
                            logger.debug("create");
                            connector.findFreeNormalized(datasetHelper, new AtomicInteger(0), ar -> {
                                if (ar.succeeded()) {
                                    datasetHelper.init(ar.result());
                                    if (Boolean.TRUE.equals(createAccessURLs)) {
                                        datasetHelper.setAccessURLs(dataUploadConnector);
                                    }
                                    createOrUpdatePromise.complete();
                                } else {
                                    createOrUpdatePromise.fail(ar.cause());
                                }
                            });
                        }
                    }
                    return createOrUpdatePromise.future();
                }).compose(v -> {
                    Promise<DatasetHelper> translationPromise = Promise.promise();
                    if (translationConfig.getBoolean("enable", false)) {


                        translateUpdate(datasetHelper, oldDataset.get()).onComplete(ar -> {
                            DatasetHelper finalHelper = ar.succeeded() ? ar.result() : datasetHelper;
                            translationPromise.complete(finalHelper);
                        });
                    } else {
                        translationPromise.complete(datasetHelper);
                    }
                    return translationPromise.future();
                }).onSuccess(finalHelper -> {
                    if (indexConfig.getBoolean("enabled", true)) {
                        index(finalHelper).onComplete(ar -> {
                            if (ar.failed()) {
                                logger.warn("Send dataset to index.", ar.cause());
                            }
                        });
                    }
                    if (!finalHelper.model().containsResource(ModelFactory.createDefaultModel().createResource(finalHelper.uriRef()))) {
                        finalHelper.model().listSubjectsWithProperty(RDF.type, DCAT.Dataset).forEachRemaining(ds -> RdfExtensionsKt.rename(ds, finalHelper.uriRef()));
                        finalHelper.model().listSubjectsWithProperty(RDF.type, DCAT.CatalogRecord).forEachRemaining(ds -> RdfExtensionsKt.rename(ds, finalHelper.recordUriRef()));
                    }
                    store(finalHelper).onComplete(sr -> {
                        if (sr.succeeded()) {
                            tripleStore.update("INSERT DATA { GRAPH <"
                                    + finalHelper.catalogueGraphName() + "> { <"
                                    + finalHelper.catalogueUriRef() + "> <http://www.w3.org/ns/dcat#record> <"
                                    + finalHelper.recordUriRef() + "> ; <http://www.w3.org/ns/dcat#dataset> <"
                                    + finalHelper.uriRef() + "> . } }");
                            if (validationConfig.getBoolean("enabled", false)) {
                                validate(finalHelper);
                            }
                            handler.handle(Future.succeededFuture(sr.result()));
                        } else {
                            handler.handle(Future.failedFuture(sr.cause()));
                        }
                    });

                }).onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
            } else {
                handler.handle(Future.failedFuture(dr.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService postDataset(String datasetString, String contentType, String catalogueId, Boolean createAccessURLs, Handler<AsyncResult<JsonObject>> handler) {

        // 0.  Get New Dataset Id
        // will not work!
        Promise<String> newDatsetIdPromise = Promise.promise();
        connector.newDatasetId(newDatsetIdPromise);
        newDatsetIdPromise.future().compose(newId -> {
            // 1. Create Dataset
            Promise<DatasetHelper> datasetPromise = Promise.promise();
            DatasetHelper.create(newId, datasetString, contentType,
                    null, catalogueId, datasetPromise);
            return datasetPromise.future();
        }).compose(dataset -> {
            // 2. Check if catalogue exists
            // A little too late if we already count on it when determining the new id
            Promise<JsonObject> catalogueExists = Promise.promise();
            connector.catalogueExists(dataset.catalogueUriRef(), catalogueExists);

            return catalogueExists.future().compose(catalogue -> {
                dataset.sourceType(catalogue.getString("type"));
                dataset.sourceLang(catalogue.getString("lang"));
                return Future.succeededFuture(dataset);
            });

        }).compose(dataset -> {
            // 3. Initialize Dataset
            dataset.init(dataset.id());
            if (createAccessURLs) {
                dataset.setAccessURLs(dataUploadConnector);
            }
            return Future.succeededFuture(dataset);
        }).compose(dataset -> {
            // 4. Translate Dataset
            Future<DatasetHelper> future;
            if (translationConfig.getBoolean("enable", false)) {
                logger.debug("Translating");
                future = translate(dataset);
            } else {
                future = Future.succeededFuture(dataset);
            }
            return future;
        }).compose(dataset -> {
            // Validate Dataset
            if (validationConfig.getBoolean("enabled", false)) {
                validate(dataset);
            }

            if (!dataset.model().containsResource(ModelFactory.createDefaultModel().createResource(dataset.uriRef()))) {
                dataset.model().listSubjectsWithProperty(RDF.type, DCAT.Dataset).forEachRemaining(ds -> RdfExtensionsKt.rename(ds, dataset.uriRef()));
                dataset.model().listSubjectsWithProperty(RDF.type, DCAT.CatalogRecord).forEachRemaining(ds -> RdfExtensionsKt.rename(ds, dataset.recordUriRef()));
            }
            return Future.succeededFuture(dataset);
        }).compose(dataset -> {
            // Index and Catalogue
            Future<DatasetHelper> indexFuture = this.index(dataset);
            Future<DatasetHelper> catalogueFuture = this.catalogue(dataset);
            return CompositeFuture.all(indexFuture, catalogueFuture).map(dataset);
        }).compose(this::store).onComplete(handler);

        return this;
    }


    @Override
    public DatasetsService deleteDataset(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        connector.getDatasetUriRefs(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                String datasetUriRef = result.getString("datasetUriRef");
                DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(datasetUriRef);

                connector.deleteGraph(datasetUriRef, dr -> {
                    if (dr.failed()) {
                        handler.handle(Future.failedFuture(ar.cause()));
                    } else {
                        indexService.deleteDataset(schema.getId(), ir -> {
                            if (ir.failed()) {
                                logger.error("Remove index", ir.cause());
                            }
                        });
                        connector.removeDatasetFromCatalogue(datasetUriRef, schema.getRecordUriRef(), res -> {
                            if (res.failed()) {
                                logger.error("Remove catalogue entries", ar.cause());
                            }
                        });
                        handler.handle(Future.succeededFuture(new JsonObject().put("status", "deleted")));
                    }
                });

                tripleStore.deleteGraph(schema.getMetricsGraphName());

            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService indexDataset(String datasetId, String catalogueId, String defaultLang, Handler<AsyncResult<JsonObject>> handler) {
        String contentType = "application/n-triples";
        getDataset(datasetId, catalogueId, contentType, ar -> {
            if (ar.succeeded()) {
                DatasetHelper.create(datasetId, ar.result().getString("content"), contentType, null, catalogueId, dr -> {
                    if (dr.succeeded()) {
                        DatasetHelper helper = dr.result();
                        JsonObject indexObject = Indexing.indexingDataset(helper.resource(), catalogueId, defaultLang);
                        indexService.addDatasetPut(indexObject, ir -> {
                            if (ir.failed()) {
                                handler.handle(Future.failedFuture(ir.cause()));
                            } else {
                                handler.handle(Future.succeededFuture());
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(dr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService getDataUploadInformation(String datasetId, String catalogueId, String resultDataset, Handler<AsyncResult<JsonObject>> handler) {
        DatasetHelper.create(resultDataset, Lang.NTRIPLES.getHeaderString(), dr -> {
            if (dr.succeeded()) {
                DatasetHelper helper = dr.result();
                JsonArray uploadResponse = dataUploadConnector.getResponse(helper);
                JsonObject result = new JsonObject();
                result.put("status", "success");
                result.put("distributions", uploadResponse);
                handler.handle(Future.succeededFuture(result));
            } else {
                handler.handle(Future.failedFuture(dr.cause()));
            }
        });
        return this;
    }

    private Future<DatasetHelper> translate(DatasetHelper helper) {
        Promise<DatasetHelper> promise = Promise.promise();
        translationService.initializeTranslationProcess(helper,null, promise);
        return promise.future();
    }

    private Future<DatasetHelper> translateUpdate(DatasetHelper helper, DatasetHelper oldHelper) {
        Promise<DatasetHelper> promise = Promise.promise();
        translationService.initializeTranslationProcess(helper,oldHelper, promise);
        return promise.future();
    }

    private void getHash(DatasetHelper helper, Handler<AsyncResult<JsonObject>> handler) {
        String query = "SELECT ?hash ?record WHERE {<" + helper.catalogueUriRef() + "> <" + DCAT.record + "> ?record. ?record <" + DCTerms.identifier + "> \"" + helper.id() + "\"; <" + SPDX.checksum + ">/<" + SPDX.checksumValue + "> ?hash . }";
        tripleStore.select(query, ar -> {
            if (ar.succeeded()) {
                ResultSet set = ar.result();
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    RDFNode hash = solution.get("hash");
                    if (hash != null && hash.isLiteral()) {
                        logger.debug("Hash available");
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("success", true)
                                .put("hash", hash.asLiteral().toString())
                                .put("recordUriRef", solution.getResource("record").toString())));
                    } else {
                        logger.debug("No old hash available");
                        handler.handle(Future.succeededFuture(new JsonObject().put("success", true)));
                    }
                } else {
                    logger.debug("No old hash available");
                    handler.handle(Future.succeededFuture(new JsonObject().put("success", false)));
                }
            } else {
                logger.error("Hash selection failed");
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<JsonObject> store(DatasetHelper helper) {
        Promise<JsonObject> promise = Promise.promise();
        datasetManager.setGraph(helper.graphName(), helper.model()).onComplete(ar -> {
            if (ar.succeeded()) {
                switch (ar.result()) {
                    case "created":
                        promise.complete(new JsonObject()
                                .put("status", "created")
                                .put("id", helper.id())
                                .put("dataset", helper.stringify(Lang.NTRIPLES))
                                .put(HttpHeaders.LOCATION, helper.uriRef()));
                        break;
                    case "updated":
                        promise.complete(new JsonObject().put("status", "updated").put(HttpHeaders.LOCATION, helper.uriRef()));
                        break;
                    default:
                        promise.fail(ar.result());
                }
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    private Future<DatasetHelper> catalogue(DatasetHelper helper) {
        Promise<DatasetHelper> datasetInCatalogue = Promise.promise();
        connector.addDatasetToCatalogue(helper.uriRef(), helper.recordUriRef(), helper.catalogueUriRef(), res -> {
            if (res.succeeded()) {
                HttpResponse<Buffer> response = res.result();
                if (response.statusCode() == 200) {
                    logger.debug("Catalogue entries created in {}", helper.catalogueUriRef());
                    datasetInCatalogue.complete(helper);
                }
            } else {
                logger.error("Adding catalogue entries to " + helper.catalogueUriRef() + ":", res.cause());
                datasetInCatalogue.fail(res.cause());
            }
        });

        return datasetInCatalogue.future();
    }

    private Future<DatasetHelper> index(DatasetHelper helper) {
        Promise<DatasetHelper> datasetIndexed = Promise.promise();
        JsonObject indexMessage = Indexing.indexingDataset(helper.resource(), helper.catalogueId(), helper.sourceLang());
        indexService.addDatasetPut(indexMessage, ar -> {
            if (ar.succeeded()) {
                datasetIndexed.complete(helper);
            } else {
                datasetIndexed.fail(ar.cause());
            }
        });

        return datasetIndexed.future();
    }

    private void validate(DatasetHelper helper) {
        if (launcher.isPipeAvailable(validationConfig.getString("metricsPipeName", "metrics-complete"))) {
            JsonObject dataInfo = new JsonObject().put("identifier", helper.id()).put("catalogue", helper.catalogueId());
            JsonObject configs = new JsonObject().put("validating-shacl", new JsonObject().put("skip", !"dcat-ap".equals(helper.sourceType())));
            metricsManager.getGraph(helper.metricsGraphName()).onComplete(ar -> {
                Dataset dataset = DatasetFactory.create(helper.model());
                if (ar.succeeded()) {
                    Model metrics = PiveauMetrics.createMetricsGraph(dataset, helper.metricsGraphName());
                    ar.result().listStatements(helper.resource(), DQV.hasQualityMeasurement, (RDFNode) null)
                            .filterKeep(statement -> statement.getResource().hasProperty(DQV.isMeasurementOf, PV.scoring))
                            .forEachRemaining(statement -> {
                                metrics.add(JenaUtils.extractResource(statement.getResource()));
                                metrics.add(helper.resource(), DQV.hasQualityMeasurement, statement.getResource());
                            });
                }
                launcher.runPipeWithData(validationConfig.getString("metricsPipeName", "metrics-complete"), JenaUtils.write(dataset, Lang.TRIG), RDFMimeTypes.TRIG, dataInfo, configs, null);
            });
        }
    }

}
