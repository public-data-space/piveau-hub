package io.piveau.hub.shell;

import io.piveau.dcatap.*;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.Constants;
import io.piveau.indexing.Indexing;
import io.piveau.json.ConfigHelper;
import io.piveau.utils.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.shell.command.Command;
import io.vertx.ext.shell.command.CommandBuilder;
import io.vertx.ext.shell.command.CommandProcess;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SyncIndexCommand {

    private IndexService indexService;

    private TripleStore tripleStore;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;

    private Command command;

    private WebClient client;

    private String ELASTICSEARCH_ADDRESS;

    public static Command create(Vertx vertx) {
        return new SyncIndexCommand(vertx).command;
    }

    private SyncIndexCommand(Vertx vertx) {
        indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        client = WebClient.create(vertx);
        ELASTICSEARCH_ADDRESS = vertx.getOrCreateContext().config().getString(Constants.ENV_PIVEAU_HUB_ELASTICSEARCH_ADDRESS, "http://elasticsearch:9200");

        tripleStore = new TripleStore(vertx, ConfigHelper.forConfig(vertx.getOrCreateContext().config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG), client);
        catalogueManager = tripleStore.getCatalogueManager();
        datasetManager = tripleStore.getDatasetManager();

        command = CommandBuilder.command(
                CLI.create("sync")
                        .addArgument(
                                new Argument()
                                        .setArgName("catalogueIds")
                                        .setRequired(false)
                                        .setDescription("The ids of the catalogues to re-index."))
                        .addOption(
                                new Option()
                                        .setArgName("exclude")
                                        .setShortName("e")
                                        .setLongName("exclude")
                                        .setDefaultValue("")
                                        .setDescription("Exclude a list of catalogues"))
                        .addOption(
                                new Option()
                                        .setArgName("partitionSize")
                                        .setShortName("p")
                                        .setLongName("partitionSize")
                                        .setDefaultValue("1000")
                                        .setDescription("Page size for partitioning datasets."))
                        .addOption(new Option().setHelp(true).setFlag(true).setArgName("help").setShortName("h").setLongName("help"))
                        .addOption(new Option().setFlag(true).setArgName("verbose").setShortName("v").setLongName("verbose"))
                        .addOption(new Option().setFlag(true).setArgName("amountOnly").setShortName("a").setLongName("amountOnly")))
                .processHandler(process -> {
                    process.session().put("syncIndexCounter", new AtomicInteger());

                    List<String> catalogueIds = process.commandLine().allArguments();
                    if (catalogueIds.isEmpty()) {
                        process.write("Reindexing all catalogues\n");
                        catalogueManager.list().onComplete(ar -> {
                            if (ar.succeeded()) {
                                syncCatalogues(ar.result(), process);
                            } else {
                                process.write("Reindexing failed: " + ar.cause().getMessage() + "\n");
                                process.end();
                            }
                        });
                    } else {
                        syncCatalogues(catalogueIds.stream().map(id -> DCATAPUriSchema.applyFor(id).getCatalogueUriRef()).collect(Collectors.toList()), process);
                    }
                })
                .build(vertx);
    }

    private void syncCatalogues(List<String> uriRefs, CommandProcess process) {
        Instant start = Instant.now();
        String exclude = process.commandLine().getOptionValue("exclude");
        List<String> excludeCatalogues = exclude.isBlank() ? Collections.emptyList() : Arrays.stream(exclude.split(",")).map(s -> DCATAPUriSchema.applyFor(s).getCatalogueUriRef()).collect(Collectors.toList());
        uriRefs.removeAll(excludeCatalogues);
        if (!uriRefs.isEmpty()) {
            Promise<Void> promise = Promise.promise();
            reduceCatalogues(uriRefs, process, promise);
            promise.future().onComplete(v -> {
                process.write("Reindexing all catalogues finished. Overall duration " + Duration.between(start, Instant.now()) + "\n");
                process.end();
            });
        } else {
            process.write("No catalogues for indexing.\n");
            process.end();
        }
    }

    private void reduceCatalogues(List<String> catalogues, CommandProcess process, Promise<Void> promise) {
        String catalogue = catalogues.remove(0);
        DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(catalogue);
        process.write("Start indexing " + schema.getId() + "\n");
        syncCatalogue(schema, process, ic -> {
            if (ic.succeeded()) {
                process.write("Reindex of " + schema.getId() + " finished. Duration " + ic.result().toString() + "\n");
            } else {
                process.write("Reindex of " + schema.getId() + " failed: " + ic.cause().getMessage() + "\n");
            }
            if (catalogues.isEmpty()) {
                promise.complete();
            } else {
                reduceCatalogues(catalogues, process, promise);
            }
        });
    }

    private void syncCatalogue(DCATAPUriRef catalogueRef, CommandProcess process, Handler<AsyncResult<Duration>> handler) {
        Instant start = Instant.now();

        int chunk = Integer.parseInt(process.commandLine().getOptionValue("partitionSize"));
        boolean verbose = process.commandLine().isFlagEnabled("verbose");

        boolean amountOnly = process.commandLine().isFlagEnabled("amountOnly");

        String queryCatalogue = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + catalogueRef.getCatalogueGraphName() + "> { ?s ?p ?o MINUS { ?s <" + DCAT.record + "> ?o } } }";
        tripleStore.construct(queryCatalogue).onComplete(ar -> {
            if (ar.succeeded()) {
                Model catalogueModel = ar.result();
                Promise<Void> cataloguePromise = Promise.promise();
                if (catalogueModel.isEmpty()) {
                    cataloguePromise.fail("No such catalogue.");
                } else if (amountOnly) {
                    cataloguePromise.complete();
                } else {
                    Promise<Void> indexPromise = Promise.promise();
                    indexService.addCatalog(Indexing.indexingCatalogue(catalogueModel.getResource(catalogueRef.getCatalogueUriRef())), cr -> {
                        if (cr.succeeded()) {
                            process.write("Indexing metadata of catalogue " + catalogueRef.getId() + " was successful.\n");
                            indexPromise.complete();
                        } else {
                            process.write("Indexing metadata of catalogue " + catalogueRef.getId() + " failed: " + cr.cause().getMessage() + "\n");
                            indexPromise.fail(cr.cause());
                        }
                    });
                    indexPromise.future().compose(v -> {
                        Promise<Void> promise = Promise.promise();
                        List<RDFNode> datasets = catalogueModel.listObjectsOfProperty(DCAT.dataset).toList();
                        process.write("Start indexing " + datasets.size() + " datasets...\n");
                        List<List<RDFNode>> partitions = Lists.partition(datasets, chunk);
                        if (verbose) {
                            process.write("Index datasets in " + partitions.size() + " partitions\n");
                        }
                        reducePartitions(catalogueRef, partitions, process, promise);
                        return promise.future();
                    }).onComplete(v -> {
                        if (v.succeeded()) {
                            process.write("\nIndexing datasets finished.\n");
                            cataloguePromise.complete();
                        } else {
                            cataloguePromise.fail(v.cause());
                        }
                    });
                }
                cataloguePromise.future().onComplete(v -> {
                    if (v.succeeded()) {
                        getIndexDatasetIds(catalogueRef.getId(), il -> {
                            if (il.succeeded()) {
                                Set<String> indexList = il.result();
                                Set<String> storeList = catalogueModel.listObjectsOfProperty(DCAT.dataset).toSet().stream().map(node -> DCATAPUriSchema.parseUriRef(node.asResource().getURI()).getId()).collect(Collectors.toSet());
                                indexList.removeAll(storeList);
                                process.write("Number of obsolete datasets in " + catalogueRef.getId() + " index: " + indexList.size() + "\n");
                                indexList.forEach(id -> {
                                    indexService.deleteDataset(id, dr -> {
                                        if (dr.failed()) {
                                            process.write(dr.cause().getMessage());
                                        }
                                    });
                                });
                                handler.handle(Future.succeededFuture(Duration.between(start, Instant.now())));
                            } else {
                                process.write(il.cause().getMessage());
                                handler.handle(Future.failedFuture(il.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(v.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void reducePartitions(DCATAPUriRef catalogueSchema, List<List<RDFNode>> partitions, CommandProcess process, Promise<Void> promise) {
        boolean verbose = process.commandLine().isFlagEnabled("verbose");

        if (partitions.isEmpty()) {
            if (verbose) {
                process.write("No more partitions\n");
            }
            promise.complete();
        } else {
            List<Future> futures = new ArrayList<>();
            List<RDFNode> partition = partitions.get(0);
            partition.forEach(node -> {
                if (node.isURIResource()) {
                    String datasetUri = node.asResource().getURI();
                    if (verbose) {
                        process.write("Index dataset " + datasetUri + "\n");
                    }
                    Promise<Void> datasetPromise = Promise.promise();
                    futures.add(datasetPromise.future());
                    datasetManager.getGraph(node.asResource().getURI()).onComplete(dr -> {
                        if (dr.succeeded()) {
                            if (verbose) {
                                process.write("Dataset " + datasetUri + " fetched successfully\n");
                            }
                            Model dataset = dr.result();
                            if (dataset.isEmpty()) {
                                // remove from catalogue...
                                String deleteQuery = "DELETE DATA { GRAPH <" + catalogueSchema.getCatalogueGraphName() + ">"
                                        + "{ <" + catalogueSchema.getCatalogueUriRef() + "> <" + DCAT.dataset + "> "
                                        + "<" + datasetUri + "> ; <" + DCAT.record + "> <" + DCATAPUriSchema.parseUriRef(datasetUri).getRecordUriRef() + "> } }";
                                tripleStore.update(deleteQuery).onComplete(rr -> {
                                    if (rr.succeeded()) {
                                        process.write("\nDataset " + datasetUri + " removed from catalogue.\n");
                                        datasetPromise.complete();
                                    } else {
                                        process.write("\nCould not remove " + datasetUri + " from catalogue: " + rr.cause().getMessage() + "\n");
                                        datasetPromise.fail(rr.cause());
                                    }
                                });
                                indexService.deleteDataset(datasetUri, ir -> {
                                    if (ir.succeeded()) {
                                        process.write("\nDataset " + datasetUri + " removed from index.\n");
                                    } else {
                                        process.write("\nCould not remove " + datasetUri + " from index: " + ir.cause().getMessage() + "\n");
                                    }
                                });
                            } else {
                                try {
                                    JsonObject indexInfo = Indexing.indexingDataset(dataset.getResource(datasetUri), catalogueSchema.getId(), "de");
                                    if (!indexInfo.isEmpty()) {
                                        if (verbose) {
                                            process.write("Index info for " + datasetUri + " generated successfully\n");
                                        }
                                        indexService.addDatasetPut(indexInfo, ir -> {
                                            if (ir.failed()) {
                                                process.write("\nSent indexed dataset " + datasetUri + " failed: " + indexInfo.encodePrettily() + " - " + ir.cause() + "\n");
                                                datasetPromise.fail(ir.cause());
                                            } else {
                                                if (verbose) {
                                                    process.write("Dataset " + datasetUri + " indexed successfully\n");
                                                }
                                                datasetPromise.complete();
                                            }
                                        });
                                    } else {
                                        if (verbose) {
                                            process.write("Index info for " + datasetUri + " was empty!\n");
                                        }
                                        datasetPromise.fail("Index info for " + datasetUri + " was empty!");
                                    }
                                } catch (Exception e) {
                                    if (verbose) {
                                        process.write("Indexing dataset " + datasetUri + " failed (" + e.getMessage() + "): " + JenaUtils.write(dataset, Lang.TURTLE) + "\n");
                                    } else {
                                        process.write("\nIndexing dataset " + datasetUri + " failed: " + e.getMessage() + "\n");
                                    }
                                    datasetPromise.fail(e);
                                }
                            }
                        } else {
                            process.write("\nFailed to fetch " + datasetUri + ": " + dr.cause().getMessage() + "\n");
                            datasetPromise.fail(dr.cause());
                        }
                    });
                }
            });
            CompositeFuture.join(futures).onComplete(cf -> {
                if (!verbose) {
                    AtomicInteger counter = process.session().get("syncIndexCounter");
                    process.write("\rIndexed " + counter.addAndGet(partition.size()));
                }
                reducePartitions(catalogueSchema, partitions.subList(1, partitions.size()), process, promise);
            });
        }
    }

    private void getIndexDatasetIds(String catalogueId, Handler<AsyncResult<Set<String>>> handler) {
        HttpRequest<Buffer> request = client.getAbs(ELASTICSEARCH_ADDRESS + "/dataset/_search")
                .addQueryParam("q", "catalog.id:" + catalogueId)
                .addQueryParam("_source_includes", "id")
                .addQueryParam("size", "250000");

        request.send(ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result().bodyAsJsonObject();
                JsonArray hits = result.getJsonObject("hits", new JsonObject()).getJsonArray("hits", new JsonArray());
                Set<String> ids = hits.stream().map(obj -> ((JsonObject) obj).getJsonObject("_source").getString("id")).collect(Collectors.toSet());
                handler.handle(Future.succeededFuture(ids));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

}
