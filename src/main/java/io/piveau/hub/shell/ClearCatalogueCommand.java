package io.piveau.hub.shell;

import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.Constants;
import io.piveau.dcatap.CatalogueManager;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.DatasetManager;
import io.piveau.dcatap.TripleStore;
import io.piveau.dcatap.DCATAPUriRef;
import io.piveau.dcatap.DCATAPUriSchema;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.ext.shell.command.Command;
import io.vertx.ext.shell.command.CommandBuilder;
import io.vertx.ext.shell.command.CommandProcess;
import kotlin.Unit;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;

import java.util.ArrayList;
import java.util.List;

public class ClearCatalogueCommand {

    private Command command;

    private IndexService indexService;

    private TripleStore tripleStore;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;

    private ClearCatalogueCommand(Vertx vertx) {
        indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        tripleStore = new TripleStore(vertx, ConfigHelper.forConfig(vertx.getOrCreateContext().config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG), null);
        catalogueManager = tripleStore.getCatalogueManager();
        datasetManager = tripleStore.getDatasetManager();

        command = CommandBuilder.command(
                CLI.create("clear")
                        .addArgument(
                                new Argument()
                                        .setArgName("catalogueId")
                                        .setRequired(true)
                                        .setDescription("The id of the catalogues to clear."))
                        .addOption(new Option().setHelp(true).setFlag(true).setArgName("help").setShortName("h").setLongName("help"))
                        .addOption(new Option().setFlag(true).setArgName("keepIndex").setShortName("k").setLongName("keepIndex"))
                        .addOption(new Option().setFlag(true).setArgName("verbose").setShortName("v").setLongName("verbose"))
        ).processHandler(process -> {
            CommandLine commandLine = process.commandLine();
            clearCatalogue(process, commandLine.getArgumentValue(0), commandLine.isFlagEnabled("keepIndex"));
        }).build(vertx);
    }

    public static Command create(Vertx vertx) {
        return new ClearCatalogueCommand(vertx).command;
    }

    private void clearCatalogue(CommandProcess process, String catalogueId, boolean keepIndex) {
        DCATAPUriRef catalogueSchema = DCATAPUriSchema.applyFor(catalogueId);

        process.write("Start clearing catalogue " + catalogueSchema.getId() + (keepIndex ? ", keeping index.\n" : "\n"));

        catalogueManager.get(catalogueId).onComplete(ar -> {
            if (ar.succeeded()) {
                Model catalogueModel = ar.result();
                Resource catalogue = catalogueModel.getResource(catalogueSchema.getCatalogueUriRef());

                List<Resource> datasets = catalogueModel.listObjectsOfProperty(DCAT.dataset).mapWith(RDFNode::asResource).toList();
                process.write("Found " + datasets.size() + " datasets\n");

                List<List<Resource>> partitions = Lists.partition(datasets, 1000);
                Promise<Void> partitionsPromise = Promise.promise();
                nextPartition(catalogue, partitions, 0, process, partitionsPromise);
                partitionsPromise.future().onComplete(pr -> {
                    if (pr.succeeded()) {
                        catalogueManager.set(catalogueId, catalogueModel).onComplete(gr -> {});
                        process.write("Catalogue " + catalogueId + " cleared.\n");
                    } else {
                        process.write("Clear catalogue failed: " + pr.cause().getMessage() + "\n");
                    }
                    process.end();
                });
            } else {
                process.write("Can't get catalogue " + catalogueId + ": " + ar.cause().getMessage() + "\n");
            }
        });

    }

    private void nextPartition(Resource catalogue, List<List<Resource>> partitions, int index, CommandProcess process, Promise<Void> promise) {
        if (index >= partitions.size()) {
            process.write("No more partitions\n");
            promise.complete();
        } else {
            List<Future<Void>> futures = new ArrayList<>();
            List<Resource> partition = partitions.get(index);
            partition.forEach(dataset -> {
                DCATAPUriRef datasetSchema = DCATAPUriSchema.parseUriRef(dataset.asResource().getURI());
                Promise<Void> datasetPromise = Promise.promise();
                futures.add(datasetPromise.future());
                datasetManager.deleteGraph(datasetSchema.getDatasetGraphName(), dr -> {
                    if (dr.succeeded()) {
                        process.write("Dataset " + datasetSchema.getId() + " removed from triple store\n");

                        tripleStore.deleteGraph(datasetSchema.getMetricsGraphName());

                        if (!process.commandLine().isFlagEnabled("keepIndex")) {
                            indexService.deleteDataset(datasetSchema.getId(), ir -> {
                                if (ir.failed()) {
                                    process.write("Dataset " + datasetSchema.getId() + " could not be removed from index: " + ir.cause().getMessage() + "\n");
                                }
                            });
                        }

                        datasetPromise.complete();
                    } else {
                        datasetPromise.fail(dr.cause());
                    }
                    return Unit.INSTANCE;
                });
                Model catalogueModel = catalogue.getModel();
                catalogueModel
                        .remove(catalogue, DCAT.dataset, catalogueModel.createResource(datasetSchema.getDatasetUriRef()))
                        .remove(catalogue, DCAT.record, catalogueModel.createResource(datasetSchema.getRecordUriRef()));
            });
            CompositeFuture.join(new ArrayList<>(futures)).onComplete(cf ->
                nextPartition(catalogue, partitions, index + 1, process, promise));
        }
    }

}
