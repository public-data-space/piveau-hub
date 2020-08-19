package io.piveau.hub.util;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.utils.DigestAuth;
import io.piveau.utils.JenaUtils;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.vocabularies.Concept;
import io.piveau.vocabularies.Languages;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.*;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TSConnector {


    private WebClient client;

    private String uri;

    private String username;
    private String password;
    private String dataEndpoint;
    private String queryEndpoint;
    private String updateEndpoint;

    private CircuitBreaker breaker;

    public static TSConnector create(WebClient client, CircuitBreaker breaker, JsonObject config) {
        return new TSConnector(client, breaker, config);
    }

    private TSConnector(WebClient client, CircuitBreaker breaker, JsonObject config) {
        this.client = client;
        this.breaker = breaker;
        this.uri = config.getString("address", "http://piveau-virtuoso:8890");
        this.username = config.getString("username", "dba");
        this.password = config.getString("password", "dba");
        this.dataEndpoint = config.getString("graphAuthEndpoint", "/sparql-graph-crud-auth");
        this.queryEndpoint = config.getString("queryEndpoint", "/sparql");
        this.updateEndpoint = config.getString("queryAuthEndpoint", "/sparql-auth");
    }

    public void getDistribution(String uriRef, String accept, Handler<AsyncResult<String>> handler) {
        //PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        //TODO: this might not return the full distribution
        ConstructBuilder cb = new ConstructBuilder()
                .addConstruct("<" + uriRef + ">", "?q", "?x")
                .addConstruct("?x", "?p", "?y").addGraph("?g", new WhereBuilder()
                .addWhere("<" + uriRef + ">", "?q", "?x")
                .addOptional("?x", "?p", "?y"));


        //log.info("query: {}", cb.buildString());
        // TODO Actual: special exception handling for the content-type application/ld+json that comes without a language specification from virtuoso
        String acceptHeader = accept == null || accept.isEmpty() || accept.equals("application/ld+json") ? "application/n-triples" : accept;
        query(cb.buildString(), acceptHeader, ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    if (accept != null && accept.equals("application/ld+json")) {
                        // transform back to json-ld from n-triples
                        Model model = JenaUtils.read(ar.result().bodyAsString().getBytes(), "application/n-triples");
                        handler.handle(Future.succeededFuture(JenaUtils.write(model, RDFFormat.JSONLD_FLATTEN_PRETTY)));
                    } else {
                        handler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                    }
                } else {
                    handler.handle(Future.failedFuture(ar.result().bodyAsString()));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }

    //Todo: it could be possible that an identifier is used in multiple Distributions in different datasets.
    public void getDistributionUriRefByIdentifier(String identifier, Handler<AsyncResult<JsonObject>> handler) {
        //PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());

        String query = "select distinct ?uri where {GRAPH ?g { ?uri <http://purl.org/dc/terms/identifier> \"" + identifier + "\". ?uri a <" + DCAT.Distribution.getURI() + ">. }} LIMIT 10";


        query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    Resource distributionUriRef = solution.getResource("uri");
                    if (distributionUriRef != null) {
                        handler.handle(Future.succeededFuture(new JsonObject().put("status", "success").put("distributionUriRef", distributionUriRef.getURI())));
                    } else {
                        handler.handle(Future.succeededFuture(new JsonObject().put("status", "not found").put("content", "Dataset with identifier " + identifier + " not found")));
                    }

                } else {
                    handler.handle(Future.succeededFuture(new JsonObject().put("status", "not found").put("content", "Dataset with identifier " + identifier + " not found")));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }


    public void getRecord(String graphName, String record, String accept, Handler<AsyncResult<String>> handler) {
        Promise<String> promise = Promise.promise();
        getGraph(graphName, accept, promise);
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());

        promise.future().onComplete(ar -> {
            if (ar.succeeded()) {

                Model originalModel = JenaUtils.read(ar.result().getBytes(), accept);
                Model recordModel = JenaUtils.extractResource(originalModel.getResource(record));

                String data = "";
                try {
                    data = JenaUtils.write(recordModel, accept);
                } catch (Exception e) {
                    log.warn("Error: " + e.getLocalizedMessage());
                    handler.handle(Future.failedFuture(e));
                }

                if (data.isEmpty()) {
                    handler.handle(Future.failedFuture("not found"));
                } else {
                    handler.handle(Future.succeededFuture(data));
                }

            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }

    public void getDatasetUriRefs(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null || catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture("Catalogue id is required, when using the non-normalized id"));
            return;
        }
        String catalogueUriRef = DCATAPUriSchema.applyFor(catalogueId).getCatalogueUriRef();
        String query = "SELECT ?dataset ?record WHERE { GRAPH ?a {<" + catalogueUriRef + "> <" + DCAT.record + "> ?record.} GRAPH ?b { ?record <" + DCTerms.identifier + "> \"" + datasetId + "\"; <" + FOAF.primaryTopic + "> ?dataset. }}";
        query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    Resource datasetUriRef = solution.getResource("dataset");
                    Resource recordUriRef = solution.getResource("record");
                    Resource validationUriRef = solution.getResource("validation");
                    JsonObject result = new JsonObject().put("datasetUriRef", datasetUriRef.getURI()).put("recordUriRef", recordUriRef.getURI());
                    if (validationUriRef != null) {
                        result.put("validationUriRef", validationUriRef.getURI());
                    }
                    handler.handle(Future.succeededFuture(result));
                } else {
                    handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 404, "Dataset with id " + datasetId + " not found")));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }


    public void getDatasetId(String datasetUriRef, Handler<AsyncResult<String>> handler) {
        String query = "SELECT ?identifier WHERE {GRAPH ?G {  ?record <" + FOAF.primaryTopic + "> <" + datasetUriRef + ">. ?record <" + DCTerms.identifier + "> ?identifier }}";
        query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    String identifier = solution.getLiteral("identifier").getString();
                    handler.handle(Future.succeededFuture(identifier));
                } else {
                    handler.handle(Future.failedFuture("Dataset with uriRef " + datasetUriRef + " not found"));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getCatalogueUriRefForDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        String datasetUriRef = DCATAPUriSchema.applyFor(datasetId).getDatasetUriRef();
        String query = "SELECT ?catalogueuri WHERE {GRAPH ?G {  ?catalogueuri <" + DCAT.dataset + "> <" + datasetUriRef + ">.  }}";

        query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    String identifier = solution.getResource("catalogueuri").getURI();
                    handler.handle(Future.succeededFuture(new JsonObject().put("identifier", identifier).put("status", "success")));
                } else {
                    handler.handle(Future.succeededFuture(
                            new JsonObject()
                                    .put("status", "not found")
                                    .put("content", "Catalog for Dataset with uriRef " + datasetUriRef + " not found")));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getDatasetUriRefForDistribution(String distributionId, Handler<AsyncResult<JsonObject>> handler) {
        String distributionUriRef = DCATAPUriSchema.applyFor(distributionId).getDistributionUriRef();
        String query = "SELECT ?dataseturi WHERE { GRAPH ?G { ?dataseturi <" + DCAT.distribution + "> <" + distributionUriRef + ">.  }}";

        query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    String identifier = solution.getResource("dataseturi").getURI();
                    handler.handle(Future.succeededFuture(new JsonObject().put("identifier", identifier).put("status", "success")));
                } else {
                    handler.handle(Future.succeededFuture(
                            new JsonObject()
                                    .put("status", "not found")
                                    .put("content", "Dataset for Distribution with uriRef " + distributionUriRef + " not found")));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getGraph(String graphName, Handler<AsyncResult<Model>> handler) {
        HttpRequest<Buffer> request = client
                .getAbs(uri + dataEndpoint)
                .putHeader("Accept", "application/n-triples")
                .addQueryParam("graph", graphName);

        Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();
        send(request, HttpMethod.GET, responsePromise);

        responsePromise.future().onComplete(ar -> {
            if (ar.succeeded()) {
                try {
                    Model model = JenaUtils.read(ar.result().body().getBytes(), "application/n-triples");
                    handler.handle(Future.succeededFuture(model));
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getGraph(String graphName, String accept, Handler<AsyncResult<String>> handler) {
        // TODO Actual: special exception handling for the content-type application/ld+json that comes without a language specification from virtuoso
        String acceptHeader = accept == null || accept.isEmpty() || accept.equals("application/ld+json") ? "application/n-triples" : accept;

        HttpRequest<Buffer> request = client
                .getAbs(uri + dataEndpoint)
                .putHeader("Accept", acceptHeader)
                .addQueryParam("graph", graphName);

        Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();
        send(request, HttpMethod.GET, responsePromise);
        responsePromise.future().onComplete(ar -> {
            if (ar.succeeded()) {
                if (accept != null && accept.equals("application/ld+json")) {
                    // transform back to json-ld from application/n-triples
                    Model model = JenaUtils.read(ar.result().bodyAsString().getBytes(), "application/n-triples");
                    handler.handle(Future.succeededFuture(JenaUtils.write(model, "application/ld+json")));
                } else {

                    handler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void putGraph(String graphName, Model model, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .putAbs(uri + dataEndpoint)
                .putHeader("Content-Type", "application/n-triples")
                .addQueryParam("graph", graphName);

        String output = JenaUtils.write(model, Lang.NTRIPLES);

        if (breaker != null) {
            breaker.<HttpResponse<Buffer>>execute(promise -> sendBuffer(request, HttpMethod.PUT, Buffer.buffer(output), promise))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            handler.handle(Future.succeededFuture(ar.result()));
                        } else {
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
        } else {
            Promise<HttpResponse<Buffer>> promise = Promise.promise();
            sendBuffer(request, HttpMethod.PUT, Buffer.buffer(output), promise);
            promise.future().onComplete(ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }

    public void deleteGraph(String graphName, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        HttpRequest<Buffer> request = client
                .deleteAbs(uri + dataEndpoint)
                .addQueryParam("graph", graphName);

        Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();

        send(request, HttpMethod.DELETE, responsePromise);

        responsePromise.future().onComplete(ar -> {
            if (ar.succeeded()) {
                log.trace("Delete succeeded: {}", graphName);
                handler.handle(Future.succeededFuture());
            } else {
                log.error("Delete failed : " + graphName, ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void catalogueExists(String catalogueUriRef, Handler<AsyncResult<JsonObject>> handler) {
        String query = "SELECT ?type ?lang WHERE { GRAPH <" + catalogueUriRef + "> { <" + catalogueUriRef + "> <" + DCTerms.type + "> ?type; <" + DCTerms.language + "> ?lang . } }";
        query(query, "application/json", ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    ResultSet result = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().bodyAsJsonObject().toString().getBytes()));
                    if (result.hasNext()) {
                        QuerySolution solution = result.next();
                        JsonObject info = new JsonObject();
                        if (solution.contains("type")) {
                            info.put("type", solution.getLiteral("type").toString());
                        }
                        if (solution.contains("lang")) {
                            Concept concept = Languages.INSTANCE.getConcept(solution.getResource("lang"));
                            if (concept != null) {
                                String langCode = Languages.INSTANCE.iso6391Code(concept);
                                if (langCode == null) {
                                    langCode = Languages.INSTANCE.tedCode(concept).toLowerCase();
                                }
                                info.put("lang", langCode);
                            }
                        }
                        handler.handle(Future.succeededFuture(info));
                    } else {
                        handler.handle(Future.failedFuture("Catalogue does not exist or has no type and lang"));
                    }
                } else {
                    handler.handle(Future.failedFuture("Catalogue exists: " + response.statusCode() + " - " + response.statusMessage()));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void addDatasetToCatalogue(String datasetUri, String recordUri, String catalogueUri, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        String query = "DELETE  { GRAPH <" + catalogueUri + "> {\n" +
                "?d <" + DCAT.dataset + "> <" + datasetUri + ">. \n" +
                "?r <" + DCAT.record + "> <" + recordUri + "> . } }\n" +
                "WHERE { GRAPH <" + catalogueUri + "> {\n" +
                "?d <" + DCAT.dataset + "> <" + datasetUri + ">. \n" +
                "?r  <" + DCAT.record + "> <" + recordUri + "> . } };\n" +
                "\n" +
                "INSERT Data { GRAPH <" + catalogueUri + ">  \n" +
                "{ <" + catalogueUri + ">  <" + DCAT.dataset + "> <" + datasetUri + ">; \n" +
                "<" + DCAT.record + "> <" + recordUri + "> . } }";

        update(query, "application/json", ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void removeDatasetFromCatalogue(String datasetUri, String recordUri, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        String catalogueQuery = "SELECT DISTINCT ?catalogue { GRAPH ?catalogue { ?s <" + DCAT.dataset + "> <" + datasetUri + "> } }";
        PiveauLogger log = PiveauLoggerFactory.getDatasetLogger(DCATAPUriSchema.parseUriRef(datasetUri).getId(), getClass());
        query(catalogueQuery, "application/json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                set.forEachRemaining(cat -> {
                    Resource catRes = cat.getResource("catalogue");
                    String query = "DELETE DATA { GRAPH <" + catRes + "> { <" + catRes + "> <" + DCAT.dataset + "> <" + datasetUri + ">; <" + DCAT.record + "> <" + recordUri + ">. } }";
                    update(query, null, handler);
                });
            } else {
                log.error("Remove catalogue entries", ar.cause());
            }
        });
    }

    public void listCatalogs(String accept, Integer limit, Integer offset, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        String query = "SELECT DISTINCT ?g { GRAPH ?g { ?g <" + RDF.type + "> <" + DCAT.Catalog + "> } } LIMIT " + (limit != null ? limit : 100);
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        query(query, accept, handler);
    }

    public void listDatasets(String accept, String catalogueUriRef, Integer limit, Integer offset, Handler<AsyncResult<String>> handler) {

        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(catalogueUriRef != null ? DCATAPUriSchema.parseUriRef(catalogueUriRef).getId() : null, getClass());
        String query = "SELECT DISTINCT ?ds { GRAPH ";

        //if we have a catalogue id , we get that catalogue and return all of its datasets
        //if we have no catalogue id, we just get all graphs, that have a Dataset that has the same uri as the graph
        if (catalogueUriRef != null) {
            query += "<" + catalogueUriRef + "> { <" + catalogueUriRef + "> <" + DCAT.dataset + "> ?ds ";
        } else {
            query += "?ds { ?s a <" + DCAT.Dataset + ">";
        }
        query += " } } LIMIT " + (limit != null ? limit : 100);

        if (offset != null) {
            query += " OFFSET " + offset;
        }

        query(query, "application/json", ar -> {
            if (ar.succeeded()) {
                InputStream is = new ByteArrayInputStream(ar.result().body().getBytes());
                ResultSet resultSet = ResultSetFactory.fromJSON(is);
                Model dm = ModelFactory.createDefaultModel();
                List<Future<Void>> futures = new ArrayList<>();
                resultSet.forEachRemaining(querySolution -> {
                    Promise<Void> promise = Promise.promise();
                    futures.add(promise.future());
                    Resource resource = querySolution.getResource("ds");
                    //log.info("DATASET URI: {}", resource.getURI());
                    getGraph(resource.getURI(), h -> {
                        if (h.succeeded()) {
                            //log.info("GOT DATASET URI: {}", resource.getURI());
                            dm.add(JenaUtils.extractResource(h.result().getResource(resource.getURI())));
                            promise.complete();
                        } else {
                            log.error("DID NOT GET DATASET URI: {}", resource.getURI());
                            promise.fail("Could not get Graph: " + resource.getURI());
                        }
                    });

                });
                CompositeFuture.all(new ArrayList<>(futures))
                        .onSuccess(v -> handler.handle(Future.succeededFuture(JenaUtils.write(dm, accept))))
                        .onFailure(cause -> handler.handle(Future.failedFuture(cause)));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void listDatasetSources(String catalogueUriRef, Handler<AsyncResult<JsonArray>> handler) {

        boolean isNotNull = catalogueUriRef != null;
        String cat = isNotNull && DCATAPUriSchema.parseUriRef(catalogueUriRef) != null ? DCATAPUriSchema.parseUriRef(catalogueUriRef).getId() : null;
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(cat, getClass());


        catalogueUriRef = catalogueUriRef != null ? "<" + catalogueUriRef + ">" : "?c";

        String queryCount = "select (count(distinct ?i) as ?count) where {GRAPH " + catalogueUriRef + " { " + catalogueUriRef + " <" + DCAT.record + "> ?r .} GRAPH ?g { ?r <" + DCTerms.identifier + "> ?i }}";
        String tempQuery = "select distinct ?s where {GRAPH " + catalogueUriRef + " { " + catalogueUriRef + " <" + DCAT.record + "> ?r .} GRAPH ?g { ?r <" + DCTerms.identifier + "> ?s }}";

        final String query = tempQuery;
        log.debug("catalogue: {}", catalogueUriRef);
        log.debug("LIST SOURCES QUERY STRING: {}", tempQuery);
        log.debug("COUNT SOURCES QUERY STRING: {}", queryCount);

        query(queryCount, "application/json", ar -> {
            if (ar.succeeded()) {
                InputStream is = new ByteArrayInputStream(ar.result().body().getBytes());
                ResultSet resultSet = ResultSetFactory.fromJSON(is);

                JsonArray jsonArray = new JsonArray();

                resultSet.forEachRemaining(querySolution -> {
                    try {
                        Literal subject = querySolution.getLiteral("count");
                        if (subject != null) {
                            log.debug("SOURCES COUNT SOLUTION: {}", subject.getInt());
                            enumSourceIds(subject.getInt(), null, query, jsonArray, handler);
                        } else {
                            log.warn("Subject is null");
                            handler.handle(Future.failedFuture("could not find IDs"));
                        }
                    } catch (ClassCastException cce) {
                        log.warn("could not cast {} to literal", querySolution.get("s").toString());
                    } catch (Exception e) {
                        log.error("error while traversing resultSet: ", e);
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void enumSourceIds(Integer count, Integer place, String query, JsonArray jsonArray, Handler<AsyncResult<JsonArray>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        String tempQuery = query;
        if (count > 10000) {
            tempQuery += " LIMIT 10000";
        }

        if (place != null) {
            tempQuery += " OFFSET " + place;
        }

        query(tempQuery, "application/json", ar -> {
            if (ar.succeeded()) {
                InputStream is = new ByteArrayInputStream(ar.result().body().getBytes());
                ResultSet resultSet = ResultSetFactory.fromJSON(is);
                resultSet.forEachRemaining(querySolution -> {
                    RDFNode subject = querySolution.get("s");
                    if (subject != null && subject.isLiteral()) {
                        jsonArray.add(subject.asLiteral().toString());
                    } else {
                        log.warn("No subject found");
                    }
                });

                if (place == null && count > 10000) {
                    enumSourceIds(count, 10000, query, jsonArray, handler);
                } else if (place != null && count > place) {
                    enumSourceIds(count, place + 10000, query, jsonArray, handler);
                } else {
                    log.debug("RETURNing IDS: {}", jsonArray.encodePrettily());
                    handler.handle(Future.succeededFuture(jsonArray));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getDatasetsAndRecords(String catalogue, Handler<AsyncResult<Map<String, List<Resource>>>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(DCATAPUriSchema.parseUriRef(catalogue).getId(), getClass());
        String datasetQuery = "SELECT DISTINCT ?dataset  \n" +
                "{ \n" +
                "\tGRAPH  <" + catalogue + "> \n" +
                "\t{ \n" +
                "\t\t<" + catalogue + "> <" + DCAT.dataset + "> ?dataset.\n" +
                "\t\t\n" +
                "\t} \n" +
                "}";

        String recordQuery = "SELECT DISTINCT ?record \n" +
                "{ \n" +
                "\tGRAPH  <" + catalogue + "> \n" +
                "\t{ \n" +
                "\t\t<" + catalogue + "> <" + DCAT.record + "> ?record.\n" +
                "\t\t\n" +
                "\t} \n" +
                "}";


        String datasetCountQuery = "SELECT (count(DISTINCT ?ds) as ?count) { GRAPH  <" + catalogue + "> { <" + catalogue + "> <" + DCAT.dataset + "> ?ds.} }";
        String recordCountQuery = "SELECT (count(DISTINCT ?ds) as ?count) { GRAPH  <" + catalogue + "> { <" + catalogue + "> <" + DCAT.record + "> ?ds.} }";

        Promise<List<Resource>> dsPromise = Promise.promise();
        Promise<List<Resource>> rsPromise = Promise.promise();
        Map<String, List<Resource>> resourceList = new HashMap<>();
        resourceList.put("dataset", new ArrayList<>());
        resourceList.put("record", new ArrayList<>());

        //get datasets
        //log.info("datasetCountQuery: {}", datasetCountQuery);
        query(datasetCountQuery, "application/json", ar -> {
            if (ar.succeeded()) {
                InputStream is = new ByteArrayInputStream(ar.result().body().getBytes());
                ResultSet resultSet = ResultSetFactory.fromJSON(is);
                resultSet.forEachRemaining(querySolution -> {
                    try {
                        Literal subject = querySolution.getLiteral("count");
                        if (subject != null) {
                            log.debug("Counted ds: {}", subject.getInt());
                            enumResources(subject.getInt(), 0, datasetQuery, "dataset", resourceList.get("dataset"), dsPromise);
                        } else {
                            log.warn("subject is null");
                            dsPromise.handle(Future.failedFuture("could not find IDs"));
                        }
                    } catch (ClassCastException cce) {
                        log.warn("could not cast {} to literal", querySolution.get("s").toString());
                    } catch (Exception e) {
                        log.error("error while traversing resultSet: ", e);
                    }
                });
            } else {
                dsPromise.handle(Future.failedFuture(ar.cause()));
                if (ar.result() != null && ar.result().body() != null) {
                    log.error(ar.result().bodyAsString());
                }
            }
        });

        //get records
        query(recordCountQuery, "application/json", ar -> {
            if (ar.succeeded()) {
                InputStream is = new ByteArrayInputStream(ar.result().body().getBytes());
                ResultSet resultSet = ResultSetFactory.fromJSON(is);
                resultSet.forEachRemaining(querySolution -> {
                    try {
                        Literal subject = querySolution.getLiteral("count");
                        if (subject != null) {
                            log.debug("Counted rs: {}", subject.getInt());
                            enumResources(subject.getInt(), 0, recordQuery, "record", resourceList.get("record"), rsPromise);
                        } else {
                            log.warn("subject is null");
                            rsPromise.handle(Future.failedFuture("could not find IDs"));
                        }
                    } catch (ClassCastException cce) {
                        log.warn("could not cast {} to literal", querySolution.get("s").toString());
                    } catch (Exception e) {
                        log.error("error while traversing resultSet: ", e);
                    }
                });
            } else {
                rsPromise.handle(Future.failedFuture(ar.cause()));
                if (ar.result() != null && ar.result().body() != null) {
                    log.error(ar.result().bodyAsString());
                }
            }
        });

        CompositeFuture.all(dsPromise.future(), rsPromise.future()).onComplete(cohandler -> {
            if (cohandler.succeeded()) {
                handler.handle(Future.succeededFuture(resourceList));
            } else {
                handler.handle(Future.failedFuture(cohandler.cause()));
            }
        });


    }


    private void enumResources(Integer count, Integer offset, String query, String varName, List<Resource> resourceList, Handler<AsyncResult<List<Resource>>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        String tempQuery = query;
        if (count > 10000) {
            tempQuery += " LIMIT 10000";
        }

        if (offset > 0) {
            tempQuery += " OFFSET " + offset;
        }
        query(tempQuery, "application/json", ar -> {
            if (ar.succeeded()) {
                ResultSet resultSet = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));

                resultSet.forEachRemaining(querySolution -> {
                    try {

                        Resource subject = querySolution.getResource(varName);
                        if (subject != null) {

                            resourceList.add(subject);
                        } else {
                            log.warn("subject is null");
                        }


                    } catch (ClassCastException cce) {
                        log.warn("could not cast {} to literal", querySolution.get("s").toString());
                    } catch (Exception e) {
                        log.error("error while traversing resultSet: ", e);
                    }
                });

                if ((offset + 10000) < count) {
                    enumResources(count, offset + 10000, query, varName, resourceList, handler);
                } else {
                    handler.handle(Future.succeededFuture(resourceList));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void findFreeNormalized(DatasetHelper helper, AtomicInteger counter, Handler<AsyncResult<String>> handler) {
        int tempCounter = counter.getAndIncrement();
        String candidate = tempCounter == 0 ? helper.uriRef() : helper.uriRef() + "_" + tempCounter;
        String query = "ASK WHERE { GRAPH ?catalogue { ?catalogue <" + DCAT.dataset + "> <" + candidate + "> }}";
        query(query, "text/html", ar -> {
            if (ar.succeeded()) {
                boolean occupied = Boolean.parseBoolean(ar.result().bodyAsString());
                if (occupied) {
                    findFreeNormalized(helper, counter, handler);
                } else {
                    handler.handle(Future.succeededFuture(candidate.substring(candidate.lastIndexOf("/") + 1)));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    // This is really stupid!
    // First, UUID are not uriRefs
    // Second, fresh generated UUIDs will never be existing before

    public void newDatasetId(Handler<AsyncResult<String>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        String newId = UUID.randomUUID().toString();

        /* Check if ID already in Use */
        String query = "ASK WHERE { GRAPH ?catalogue { ?catalogue <" + DCAT.dataset + "> <" + newId + "> }}";
        query(query, "text/html", ar -> {
            /* QUERY SUCCEDED */
            if (ar.succeeded()) {
                boolean idAlreadyExists = Boolean.parseBoolean(ar.result().bodyAsString());
                if (idAlreadyExists) {
                    newDatasetId(handler);
                } else {
                    handler.handle(Future.succeededFuture(newId));
                }
            }
            /* QUERY FAILED */
            else {
                log.error("Could not Ask for new Dataset Id", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void sendBuffer(HttpRequest<Buffer> request, HttpMethod method, Buffer buffer, Promise<HttpResponse<Buffer>> promise) {
        request.sendBuffer(buffer, ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 401) {
                    String authenticate = DigestAuth.authenticate(response.getHeader("WWW-Authenticate"), uri, method.name(), username, password);
                    if (authenticate != null) {
                        request.putHeader("Authorization", authenticate);
                        sendBuffer(request, method, buffer, promise);
                    } else {
                        promise.fail("Could not authenticate");
                    }
                } else if (response.statusCode() == 200 || response.statusCode() == 201 || response.statusCode() == 204) {
                    promise.complete(response);
                } else {
                    promise.fail(response.statusCode() + " - " + response.statusMessage() + " - " + response.bodyAsString());
                }
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    public void query(HttpRequest<Buffer> request, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        if (breaker != null) {
            breaker.<HttpResponse<Buffer>>execute(promise -> send(request, HttpMethod.GET, promise))
                    .onSuccess(response -> handler.handle(Future.succeededFuture(response)))
                    .onFailure(cause -> handler.handle(Future.failedFuture(cause)));
        } else {
            Promise<HttpResponse<Buffer>> promise = Promise.promise();
            send(request, HttpMethod.GET, promise);
            promise.future()
                    .onSuccess(response -> handler.handle(Future.succeededFuture(response)))
                    .onFailure(cause -> handler.handle(Future.failedFuture(cause)));
        }
    }

    private void send(HttpRequest<Buffer> request, HttpMethod method, Promise<HttpResponse<Buffer>> promise) {
        request.send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 401) {
                    String authenticate = DigestAuth.authenticate(response.getHeader("WWW-Authenticate"), uri, method.name(), username, password);
                    if (authenticate != null) {
                        request.putHeader("Authorization", authenticate);
                        send(request, method, promise);
                    } else {
                        promise.fail("Could not authenticate");
                    }
                } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    promise.complete(response);
                } else {
                    promise.fail(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, response.statusCode(), response.statusMessage()));
                }
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    public void query(String query, String accept, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .getAbs(uri + queryEndpoint)
                .addQueryParam("query", query);
        if (accept != null) {
            request.putHeader("Accept", accept);
        }
        query(request, handler);
    }

    public void update(String update, String accept, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .getAbs(uri + updateEndpoint)
                .addQueryParam("query", update);
        if (accept != null) {
            request.putHeader("Accept", accept);
        }
        query(request, handler);
    }

}
