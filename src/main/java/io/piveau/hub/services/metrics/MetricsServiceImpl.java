package io.piveau.hub.services.metrics;

import io.piveau.dcatap.TripleStore;
import io.piveau.dqv.PiveauMetrics;
import io.piveau.hub.util.Constants;
import io.piveau.json.ConfigHelper;
import io.piveau.pipe.PipeLauncher;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.*;
import io.piveau.dcatap.DCATAPUriRef;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.vocabularies.vocabulary.DQV;
import io.piveau.vocabularies.vocabulary.PV;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.RDF;

import java.util.List;

public class MetricsServiceImpl implements MetricsService {

    private PipeLauncher pipeLauncher;
    private TripleStore tripleStore;
    private String scoringPipe;

    private static final String SELECT_DATASET = "SELECT ?dataset WHERE { GRAPH ?g { ?dataset <http://www.w3.org/ns/dcat#distribution> <%s> } }";
    private static final String SELECT_CATALOGUE = "SELECT ?catalogue ?identifier WHERE { GRAPH ?g1 { ?catalogue <http://www.w3.org/ns/dcat#dataset> <%s> } GRAPH ?g2 { ?record <http://xmlns.com/foaf/0.1/primaryTopic> <%s> ; <http://purl.org/dc/terms/identifier> ?identifier } }";
    private static final String SELECT_NUMBER_URLS = "select (count(?u1) as ?c1) (count(?u2) as ?c2) where { graph <%s> { { ?d1 <http://www.w3.org/ns/dcat#accessURL> ?u1 } UNION { ?d2 <http://www.w3.org/ns/dcat#downloadURL> ?u2 } } }";
    private static final String SELECT_NUMBER_STATUS = "select (count(?s1) as ?c1) (count(?s2) as ?c2) where { graph <%s> { { ?s1 <http://www.w3.org/ns/dqv#hasQualityMeasurement> ?m1 . ?m1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://piveau.eu/ns/voc#accessUrlStatusCode> } UNION { ?s2 <http://www.w3.org/ns/dqv#hasQualityMeasurement> ?m2 . ?m2 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://piveau.eu/ns/voc#downloadUrlStatusCode> } } }";

    private PiveauContext moduleContext = new PiveauContext("hub", "Metrics");

    MetricsServiceImpl(TripleStore tripleStore, JsonObject config, PipeLauncher launcher, Handler<AsyncResult<MetricsService>> readyHandler) {
        this.tripleStore = tripleStore;
        this.pipeLauncher = launcher;
        JsonObject validationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_HUB_VALIDATOR);
        scoringPipe = validationConfig.getString("scoringPipeName", "metrics-scoring");
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public MetricsService getMetric(String datasetId, String catalogueId, String contentType, Handler<AsyncResult<String>> handler) {

        tripleStore.getDatasetManager().identify(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(ar.result().getFirst().getURI());
                tripleStore.getGraph(schema.getMetricsGraphName(), gr -> {
                    if (gr.succeeded()) {
                        String result = JenaUtils.write(gr.result(), contentType);
                        handler.handle(Future.succeededFuture(result));
                    } else {
                        handler.handle(Future.failedFuture(gr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public MetricsService putMeasurement(String data, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        // We find the distribution uriref subject and work the graph from there
        Model newMeasurementModel = JenaUtils.read(data.getBytes(), contentType);
        ResIterator iterator = newMeasurementModel.listSubjectsWithProperty(DQV.hasQualityMeasurement);
        if (iterator.hasNext()) {
            iterator.forEachRemaining(m -> {
                if (m.isURIResource()) {

                    Promise<Resource> datasetPromise = Promise.promise();

                    if (DCATAPUriSchema.isDistributionUriRef(m.getURI())) {
                        tripleStore.select(String.format(SELECT_DATASET, m.getURI())).onComplete(ar -> {
                            if (ar.succeeded() && ar.result().hasNext()) {
                                datasetPromise.complete(ar.result().next().getResource("dataset"));
                            } else {
                                datasetPromise.fail("not found");
                            }
                        });
                    } else {
                        datasetPromise.complete(m);
                    }

                    datasetPromise.future().onComplete(ar -> {
                        if (ar.succeeded()) {
                            Resource dataset = ar.result();
                            DCATAPUriRef uriRef = DCATAPUriSchema.parseUriRef(dataset.getURI());
                            StringBuilder query = new StringBuilder("INSERT { GRAPH <" + uriRef.getMetricsGraphName() + "> { ");
                            // each statement is a triple so we iterate through all of them and add them to the final query
                            newMeasurementModel.listStatements().forEachRemaining(statement -> {
                                Resource subject = statement.getSubject();
                                Property predicate = statement.getPredicate();
                                RDFNode object = statement.getObject();

                                if (subject.isAnon()) {
                                    query.append("_:").append(subject.getId().toString());
                                } else {
                                    query.append("<").append(m.getURI()).append(">");
                                }

                                query.append(" <").append(predicate.getURI()).append(">");

                                if (object.isAnon()) {
                                    query.append(" ").append("_:").append(object.toString());
                                } else if (object.isLiteral()) {
                                    String dataTypeUri = object.asLiteral().getDatatypeURI();
                                    query.append(" \"").append(object.asLiteral().getLexicalForm()).append("\"");
                                    if (dataTypeUri != null) {
                                        query.append("^^<").append(object.asLiteral().getDatatypeURI()).append(">");
                                    }
                                } else {
                                    query.append(" <").append(object.asResource().getURI()).append(">");
                                }
                                query.append(" . ");
                            });
                            // This line is needed in order to create blind nodes see: https://github.com/openlink/virtuoso-opensource/issues/126*/
                            query.append("} } WHERE { SELECT * { OPTIONAL { ?s ?p ?o } } LIMIT 1 }");

                            tripleStore.update(query.toString()).onComplete(ur -> {
                                if (ur.succeeded()) {
                                    moduleContext.log().debug("Measurement added successfully.");
                                    Resource measurement = m.getPropertyResourceValue(DQV.hasQualityMeasurement);
                                    if (measurement != null) {
                                        Resource measurementOf = measurement.getPropertyResourceValue(DQV.isMeasurementOf);
                                        if (measurementOf.equals(PV.accessUrlStatusCode) || measurementOf.equals(PV.downloadUrlStatusCode)) {
                                            checkIfLastStatusCode(dataset.getURI(), uriRef.getMetricsGraphName(), uriRef.getDatasetGraphName());
                                        }
                                    }
                                    handler.handle(Future.succeededFuture(new JsonObject().put("status", "updated")));
                                } else {
                                    moduleContext.log().error("Add measurement failed.", ur.cause());
                                    handler.handle(Future.failedFuture(ur.cause()));
                                }
                            });
                        } else {
                            moduleContext.log().error("Identify dataset failed.", ar.cause());
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
                }
            });
        } else {
            handler.handle(Future.failedFuture("Bad request"));
        }
        return this;
    }

    private void checkIfLastStatusCode(String datasetUriRef, String metricsGraphName, String datasetGraphName) {
        if (!pipeLauncher.isPipeAvailable(scoringPipe)) {
            return;
        }

        Future<ResultSet> futureUrlCount = tripleStore.select(String.format(SELECT_NUMBER_URLS, datasetGraphName));
        Future<ResultSet> futureStatusCount = tripleStore.select(String.format(SELECT_NUMBER_STATUS, metricsGraphName));

        CompositeFuture.join(futureUrlCount, futureStatusCount).onComplete(ar -> {
            QuerySolution solutionUrls = futureUrlCount.result().next();
            int countUrls = solutionUrls.getLiteral("c1").getInt() + solutionUrls.getLiteral("c2").getInt();
            QuerySolution solutionStatus = futureStatusCount.result().next();
            int countStatus = solutionStatus.getLiteral("c1").getInt() + solutionStatus.getLiteral("c2").getInt();

            if (countStatus == countUrls) {
                moduleContext.log().info("Counts are equal, launching scoring pipe.");

                Future<Model> datasetFuture = tripleStore.getDatasetManager().getGraph(datasetGraphName);
                Future<Model> metricsFuture = tripleStore.getGraph(metricsGraphName);
                Future<ResultSet> catalogueFuture = tripleStore.select(String.format(SELECT_CATALOGUE, datasetUriRef, datasetUriRef));

                CompositeFuture.all(datasetFuture, metricsFuture, catalogueFuture).onComplete(dr -> {
                    if (dr.succeeded()) {
                        Dataset dataset = DatasetFactory.create(datasetFuture.result());
                        dataset.addNamedModel(metricsGraphName, metricsFuture.result());

                        QuerySolution solutionCatalogue = catalogueFuture.result().next();
                        DCATAPUriRef catalogueUriRef = DCATAPUriSchema.parseUriRef(solutionCatalogue.getResource("catalogue").getURI());
                        String identifier = solutionCatalogue.getLiteral("identifier").getLexicalForm();

                        JsonObject dataInfo = new JsonObject()
                                .put("identifier", identifier)
                                .put("catalogue", catalogueUriRef.getId())
                                .put("content", "metrics");

                        // Launch scoring pipe
                        pipeLauncher.runPipeWithData(
                                scoringPipe,
                                JenaUtils.write(dataset, Lang.TRIG),
                                RDFMimeTypes.TRIG,
                                dataInfo, new JsonObject(),
                                null).onComplete(pr -> {
                            if (pr.succeeded()) {
                                moduleContext.extend(identifier).log().info("Pipe " + scoringPipe + " for dataset launched successfully.");
                            } else {
                                moduleContext.extend(identifier).log().error("Launching pipe " + scoringPipe + " failed.", pr.cause());
                            }
                        });
                    } else {
                        moduleContext.log().error("Launching pipe " + scoringPipe + "failed", dr.cause());
                    }
                });
            } else {
                moduleContext.log().info("Counts are not equal, do nothing.");
            }
        });
    }

    @Override
    public MetricsService putMetric(String datasetUriRef, String content, String contentType, Handler<AsyncResult<String>> handler) {

        DCATAPUriRef datasetSchema = DCATAPUriSchema.parseUriRef(datasetUriRef);

        Dataset dataset = JenaUtils.readDataset(content.getBytes(), contentType);
        List<Model> metrics = PiveauMetrics.listMetricsModels(dataset);
        if (!metrics.isEmpty()) {
            Model dqvModel = metrics.get(0); // We assume there is only one named graph and it is the dqv graph

            // Rename meta object
            dqvModel.listSubjectsWithProperty(RDF.type, DQV.QualityMetadata)
                    .nextOptional()
                    .ifPresent(m -> ResourceUtils.renameResource(m, datasetSchema.getMetricsUriRef()));

            tripleStore.getMetricsManager().setGraph(datasetSchema.getMetricsGraphName(), dqvModel)
                    .onSuccess(dqv -> handler.handle(Future.succeededFuture(dqv)))
                    .onFailure(cause -> handler.handle(Future.failedFuture(cause)));
        } else {
            handler.handle(Future.failedFuture("bad request"));
        }
        return this;
    }

    @Override
    public MetricsService deleteMetric(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {

        tripleStore.getDatasetManager().identify(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(ar.result().getFirst().getURI());
                tripleStore.deleteGraph(schema.getMetricsGraphName(), dr -> {
                    if (dr.succeeded()) {
                        handler.handle(Future.succeededFuture(new JsonObject()));
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

}
