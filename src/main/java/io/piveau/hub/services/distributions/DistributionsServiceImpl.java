package io.piveau.hub.services.distributions;

import io.piveau.dcatap.TripleStore;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.indexing.Indexing;
import io.piveau.utils.JenaUtils;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.vocabularies.vocabulary.SPDX;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpHeaders;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DistributionsServiceImpl implements DistributionsService {

    private static final String DISTRIBUTION_QUERY = "CONSTRUCT { <%1$s> ?p ?o . ?o ?p2 ?o2 . } WHERE { GRAPH ?g { <%1$s> a <http://www.w3.org/ns/dcat#Distribution>; ?p ?o . OPTIONAL { ?o ?p2 ?o2 } } }";

    private final TSConnector connector;
    private final TripleStore tripleStore;
    private final IndexService indexService;
    private final TranslationService translationService;

    DistributionsServiceImpl(TSConnector connector, TripleStore tripleStore, IndexService indexService, TranslationService translationService, Handler<AsyncResult<DistributionsService>> readyHandler) {
        this.connector = connector;
        this.tripleStore = tripleStore;

        this.indexService = indexService;
        this.translationService = translationService;

        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public DistributionsService getDistribution(String id, String consumes, Handler<AsyncResult<JsonObject>> handler) {

        String uriRef = DCATAPUriSchema.applyFor(id).getDistributionUriRef();

        tripleStore.construct(String.format(DISTRIBUTION_QUERY, uriRef)).onSuccess(model -> {
            if (model.isEmpty()) {
                handler.handle(Future.succeededFuture(new JsonObject().put("status", "not found")));
            } else {
                JsonObject result = new JsonObject()
                        .put("status", "success")
                        .put("content", JenaUtils.write(model, consumes));
                handler.handle(Future.succeededFuture(result));
            }
        }).onFailure(cause -> handler.handle(Future.failedFuture(cause)));

        return this;
    }

    @Override
    public DistributionsService getDistributionByIdentifier(String identifier, String consumes, Handler<AsyncResult<JsonObject>> handler) {

        JsonObject responseObject = new JsonObject();

        Promise<JsonObject> distributionUriPromise = Promise.promise();
        connector.getDistributionUriRefByIdentifier(identifier, distributionUriPromise);
        distributionUriPromise.future().compose(distributionUri -> {
            Promise<String> distributionPromise = Promise.promise();
            if(distributionUri.getString("status", "").equals("not found")) {
                responseObject.mergeIn(distributionUri);
                distributionPromise.fail(distributionUri.getString("content", "failed getting distribution"));
                return distributionPromise.future();
            }
            connector.getDistribution(distributionUri.getString("distributionUriRef", ""), consumes, distributionPromise);
            return distributionPromise.future();

        }).onComplete(ar -> {
            if(ar.succeeded()) {

                Model dist = JenaUtils.read(ar.result().getBytes(), consumes);

                if(!dist.isEmpty()) {
                    JsonObject jsonObject = new JsonObject()
                            .put("status", "success");

                    if(consumes.equalsIgnoreCase("application/ld+json")) {
                        jsonObject.put("content", new JsonObject(ar.result()));

                    } else {
                        jsonObject.put("content", ar.result());
                    }
                    handler.handle(Future.succeededFuture(jsonObject));
                } else {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "not found")
                            .put("content", "Distribution with id " + identifier + " not found")
                    ));
                }


            } else {
                if(responseObject.containsKey("status")) {
                    handler.handle(Future.succeededFuture(responseObject));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }

        });


        return this;
    }


    @Override
    public DistributionsService postDistribution(String distribution, String datasetId, String contentType, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, DistributionsServiceImpl.class);
        Promise<JsonObject> uriPromise = Promise.promise();
        connector.getDatasetUriRefs(datasetId, catalogueId, uriPromise);
        uriPromise.future().compose(uriRefs -> {
            Promise<JsonObject> insertPromise = Promise.promise();
            String datasetUriRef = uriRefs.getString("datasetUriRef");
            insertDistribution(datasetId, catalogueId, distribution, DCATAPUriSchema.parseUriRef(datasetUriRef).getDatasetGraphName(), contentType, insertPromise);
            return insertPromise.future();
        }).onComplete(ar -> {
            if(ar.succeeded()) {
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", ar.result().getString("content", ""))
                        .put(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION, ""))
                ));
            } else {
                try {
                    ReplyException s = (io.vertx.core.eventbus.ReplyException) ar.cause();
                    if(s.failureCode() == 409) {
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "already exists")
                                .put("content", ar.cause().getMessage())
                        ));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }

                } catch(ClassCastException cce) {
                    log.warn(cce.getMessage());

                    handler.handle(Future.failedFuture(ar.cause()));
                } catch(Exception e) {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }


    /**
     * Update an existing distribution based on the dct:identifier
     *
     * @param distribution
     * @param identifier
     * @param contentType
     * @param handler
     * @return
     */
    @Override
    public DistributionsService putDistributionWithIdentifier(String distribution, String identifier, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        JsonObject responseObject = new JsonObject();
        Promise<JsonObject> distributionUriPromise = Promise.promise();
        connector.getDistributionUriRefByIdentifier(identifier, distributionUriPromise);

        distributionUriPromise.future().compose(distributionUriObject -> {
            Promise<JsonObject> graphUriPromise = Promise.promise();
            if(distributionUriObject.getString("status", "").equals("not found")) {
                responseObject.mergeIn(distributionUriObject);
                graphUriPromise.fail(distributionUriObject.getString("content", "failed getting distribution"));
                return graphUriPromise.future();
            }
            responseObject.put("distributionUriRef", distributionUriObject.getString("distributionUriRef", ""));
            String distributionID = DCATAPUriSchema.parseUriRef(distributionUriObject.getString("distributionUriRef", "")).getId();
            connector.getDatasetUriRefForDistribution(distributionID, graphUriPromise);

            return graphUriPromise.future();

        }).compose(graphUriRefObject -> {
            Promise<DatasetHelper> graphPromise = Promise.promise();
            if(graphUriRefObject.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRefObject);
                graphPromise.fail(graphUriRefObject.getString("content", "failed PUTing distribution"));
                return graphPromise.future();
            }
            String datasetUriRef = DCATAPUriSchema.parseUriRef(graphUriRefObject.getString("identifier", "")).getDatasetGraphName();
            put(distribution, responseObject.getString("distributionUriRef", ""), datasetUriRef, contentType, responseObject, graphPromise);
            return graphPromise.future();
        }).onSuccess(ds->onPutSuccess(ds, handler))
                .onFailure(err->onPutFailure(err, responseObject, handler));
        return this;
    }

    /**
     * Update an existing distribution based on the distributionID
     *
     * @param distribution
     * @param distributionID
     * @param contentType
     * @param handler
     * @return
     */
    @Override
    public DistributionsService putDistribution(String distribution, String distributionID, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        String distributionUriRef = DCATAPUriSchema.applyFor(distributionID).getDistributionUriRef();
        Promise<JsonObject> graphUriPromise = Promise.promise();
        connector.getDatasetUriRefForDistribution(distributionID, graphUriPromise);
        JsonObject responseObject = new JsonObject();


        graphUriPromise.future().compose(graphUriRef -> {
            Promise<DatasetHelper> graphPromise = Promise.promise();
            if(graphUriRef.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRef);
                graphPromise.fail(graphUriRef.getString("content", "failed PUTing distribution"));
                return graphPromise.future();
            }
            String datasetUriRef = DCATAPUriSchema.parseUriRef(graphUriRef.getString("identifier", "")).getDatasetGraphName();
            put(distribution, distributionUriRef, datasetUriRef, contentType, responseObject, graphPromise);

            return graphPromise.future();
        }).onSuccess(ds->onPutSuccess(ds, handler))
                .onFailure(err->onPutFailure(err, responseObject, handler));
        return this;
    }

    private void onPutSuccess(DatasetHelper helper,  Handler<AsyncResult<JsonObject>> handler ){

            if(helper != null) {
                connector.getCatalogueUriRefForDataset(helper.id(), catUri -> {
                    if(catUri.succeeded()) {
                        String catalogueuri = catUri.result().getString("identifier");
                        helper.catalogueId(DCATAPUriSchema.parseUriRef(catalogueuri).getId());

                        connector.catalogueExists(catalogueuri, cat -> {
                            if(cat.succeeded()) {

                                helper.sourceType(cat.result().getString("type"));
                                helper.sourceLang(cat.result().getString("lang"));

                                index(helper);
                                translate(helper);
                                handler.handle(Future.succeededFuture(new JsonObject()
                                        .put("status", "success")
                                ));
                            } else {
                                handler.handle(Future.failedFuture(cat.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(catUri.cause()));
                    }
                });
            } else {
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                ));
            }
    }

    private void onPutFailure(Throwable throwable, JsonObject responseObject, Handler<AsyncResult<JsonObject>> handler ){

            if(responseObject.containsKey("status")) {
                handler.handle(Future.succeededFuture(responseObject));
            } else {
                handler.handle(Future.failedFuture(throwable));
            }


    }

    private void put(String distribution, String distributionUriRef, String datasetUriRef, String contentType, JsonObject responseObject, Handler<AsyncResult<DatasetHelper>> handler) {


        Promise<String> graphPromise = Promise.promise();
        connector.getGraph(datasetUriRef, "application/n-triples", graphPromise);
        graphPromise.future().compose(graphModel -> {
            Promise<DatasetHelper> helperPromise = Promise.promise();
            DatasetHelper.create(graphModel, "application/n-triples", helperPromise);
            return helperPromise.future();
        }).compose(helper -> helper.updateDistribution(distribution, contentType, distributionUriRef)
        ).compose(helper -> {

            updateRecord(helper);

            Promise<DatasetHelper> storePromise = Promise.promise();
            store(helper, storePromise);
            return storePromise.future();

        }).onComplete(handler);
    }

    /**
     * Deletes a distribution based on the ID
     *
     * @param id
     * @param handler
     * @return
     */
    @Override
    public DistributionsService deleteDistribution(String id, Handler<AsyncResult<JsonObject>> handler) {
        Promise<JsonObject> graphUriPromise = Promise.promise();
        // We need to get the URI of the enveloping dataset
        connector.getDatasetUriRefForDistribution(id, graphUriPromise);
        JsonObject responseObject = new JsonObject();

        graphUriPromise.future().compose(graphUriRef -> {
            Promise<String> graphPromise = Promise.promise();
            if(graphUriRef.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRef);
                graphPromise.fail(graphUriRef.getString("content", "failed getting distribution"));
                return graphPromise.future();
            }
            // Get the Dataset
            connector.getGraph(DCATAPUriSchema.parseUriRef(graphUriRef.getString("identifier", "")).getDatasetGraphName(), "application/n-triples", graphPromise);
            return graphPromise.future();
        }).compose(graphModel -> {
            Promise<DatasetHelper> helperPromise = Promise.promise();
            // Create a DatasetHelper from the Dataset string
            DatasetHelper.create(graphModel, "application/n-triples", helperPromise);
            return helperPromise.future();
        }).compose(helper -> {
            Promise<String> withoutDistGraphPromise = Promise.promise();
            String distributionUriRef = DCATAPUriSchema.applyFor(id).getDistributionUriRef();
            // Remove the actual distribution from the model
            return helper.removeDistribution(distributionUriRef);
        }).compose(graphModel -> {
            Promise<DatasetHelper> helperPromise = Promise.promise();
            // Create a new helper, where the distribution is missing
            DatasetHelper.create(graphModel, "application/n-triples", helperPromise);
            return helperPromise.future();
        }).compose(helper -> {
            // Update the Catalog Record
            updateRecord(helper);
            Promise<DatasetHelper> storePromise = Promise.promise();
            // Store the updated dataset
            store(helper, storePromise);
            return storePromise.future();
        }).onComplete(ar -> {
            DatasetHelper helper = ar.result();
            // Update the index!
            // ToDo This is super complicated and should be refactored
            if(ar.succeeded()) {
                connector.getCatalogueUriRefForDataset(helper.id(), catUri -> {
                    if(catUri.succeeded()) {
                        String catalogueuri = catUri.result().getString("identifier");
                        helper.catalogueId(DCATAPUriSchema.parseUriRef(catalogueuri).getId());
                        connector.catalogueExists(helper.catalogueUriRef(), cat -> {
                            if(cat.succeeded()) {
                                helper.sourceType(cat.result().getString("type"));
                                helper.sourceLang(cat.result().getString("lang"));

                                index(helper);
                                handler.handle(Future.succeededFuture(new JsonObject()
                                        .put("status", "success")
                                ));
                            } else {
                                handler.handle(Future.failedFuture(cat.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(catUri.cause()));
                    }
                });

            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }


    private void insertDistribution(String id, String catalogueID, String distribution, String datasetGraphName, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        Promise<String> graphPromise = Promise.promise();


        PiveauLogger log = PiveauLoggerFactory.getLogger(id, catalogueID, DistributionsServiceImpl.class);
        String identifier = getIdentifier(distribution, contentType);

        connector.getGraph(datasetGraphName, contentType, graphPromise);
        graphPromise.future().compose(graph -> {
            Promise<DatasetHelper> helperPromise = Promise.promise();
            DatasetHelper.create(id, graph, contentType, null, catalogueID, helperPromise);
            return helperPromise.future();
        }).compose(helper -> {
            Promise<DatasetHelper> helperPromise = Promise.promise();
            helper.addDistribution(distribution, contentType, helperPromise);
            return helperPromise.future();
        }).compose(helper -> {
            Promise<DatasetHelper> storePromise = Promise.promise();
            store(helper, storePromise);
            return storePromise.future();
        }).onComplete(ar -> {


            if(ar.succeeded()) {
                DatasetHelper helper = ar.result();
                connector.catalogueExists(helper.catalogueUriRef(), cat -> {
                    if(cat.succeeded()) {
                        helper.sourceType(cat.result().getString("type"));
                        helper.sourceLang(cat.result().getString("lang"));

                        String distributionUri = getDistributionURI(identifier, helper);

                        index(helper);
                        translate(helper);
                        handler.handle(Future.succeededFuture(new JsonObject().put(HttpHeaders.LOCATION, distributionUri)));
                    } else {
                        handler.handle(Future.failedFuture(cat.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private String getDistributionURI(String identifier, DatasetHelper helper) {

        for(ResIterator it = helper.model().listSubjectsWithProperty(RDF.type, DCAT.Distribution); it.hasNext(); ) {
            Resource res = it.next();
            Statement statement = res.getProperty(DCTerms.identifier);
            boolean found = false;
            if(statement != null) {
                RDFNode obj = statement.getObject();

                if(obj.isLiteral()) {
                    found = identifier.equals(obj.asLiteral().getString());
                } else if(obj.isURIResource()) {
                    found = identifier.equals(obj.asResource().getURI());
                }
            }
            if(found) {
                return res.getURI();
            }
        }

        return "";
    }


    private String getIdentifier(String distribution, String contentType) {

        Model model = JenaUtils.read(distribution.getBytes(), contentType);
        String identifier = "";

        ResIterator it = model.listSubjectsWithProperty(RDF.type, DCAT.Distribution);
        if(it.hasNext()) {
            Resource dist = it.next();

            identifier = JenaUtils.findIdentifier(dist);
            if(identifier == null && dist.isURIResource()) {
                identifier = dist.getURI();
            }
        }
        return identifier;
    }


    private void store(DatasetHelper helper, Handler<AsyncResult<DatasetHelper>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        log.debug("Store dataset");
        connector.putGraph(helper.graphName(), helper.model(), ar -> {
            if(ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if(response.statusCode() == 200) {
                    handler.handle(Future.succeededFuture(helper));
                } else if(response.statusCode() == 201) {
                    handler.handle(Future.succeededFuture(helper));
                } else {
                    log.error("Store dataset: {}", response.statusMessage());
                    handler.handle(Future.failedFuture(response.statusMessage()));
                }
            } else {
                log.error("Store dataset", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }


    private void updateRecord(DatasetHelper helper) {

        Resource record = helper.recordResource();
        String hash = DigestUtils.md5Hex(JenaUtils.write(helper.model(), Lang.NTRIPLES));

        Resource checksum = record.getPropertyResourceValue(SPDX.checksum);
        checksum.removeAll(SPDX.checksumValue);
        checksum.addProperty(SPDX.checksumValue, hash);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }


    private void translate(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        translationService.initializeTranslationProcess(helper,null, ar -> {
            if(ar.succeeded()) {
                log.debug("Requesting a new translation for model.");
            } else if(ar.failed()) {
                log.error("Dataset could not submitted to translation service.", ar.cause());
            }
        });
    }

    private void translateUpdate(DatasetHelper helper, DatasetHelper oldHelper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        translationService.initializeTranslationProcess(helper,oldHelper, ar -> {
            if(ar.succeeded()) {
                log.debug("Requesting a new translation for model.");
            } else if(ar.failed()) {
                log.error("Dataset could not submitted to translation service.", ar.cause());
            }
        });
    }

    private void index(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        JsonObject indexMessage = Indexing.indexingDataset(helper.resource(), helper.catalogueId(), helper.sourceLang());
        indexService.addDatasetPut(indexMessage, ar -> {
            if(ar.failed()) {
                log.error("Indexing", ar.cause());
            }
        });
    }

}
