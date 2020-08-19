package io.piveau.hub.shell;

import io.piveau.hub.util.Constants;
import io.piveau.dcatap.CatalogueManager;
import io.piveau.json.ConfigHelper;
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
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;

import java.util.ArrayList;
import java.util.List;

public class RepairCatalogueCommand {

    private static final String DELETE_DATA = "DELETE DATA { GRAPH <%s> {<%s> <http://www.w3.org/ns/dcat#dataset> <%s> ; <http://www.w3.org/ns/dcat#record> <%s> . } }";
    private static final String INSERT_RECORD_DATA = "INSERT DATA { GRAPH <%s> { <%s> <http://www.w3.org/ns/dcat#record> <%s> . } }";
    private static final String DELETE_RECORD_DATA = "DELETE DATA { GRAPH <%s> { <%s> <http://www.w3.org/ns/dcat#record> <%s> . } }";

    private Command command;

    private TripleStore tripleStore;
    private CatalogueManager catalogueManager;

    private RepairCatalogueCommand(Vertx vertx) {
        tripleStore = new TripleStore(vertx, ConfigHelper.forConfig(vertx.getOrCreateContext().config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG), null);
        catalogueManager = tripleStore.getCatalogueManager();

        command = CommandBuilder.command(
                CLI.create("repair")
                        .addArgument(
                                new Argument()
                                        .setArgName("catalogueId")
                                        .setRequired(true)
                                        .setDescription("The id of the catalogues to repair."))
                        .addOption(new Option().setHelp(true).setFlag(true).setArgName("help").setShortName("h").setLongName("help"))
                        .addOption(new Option().setFlag(true).setArgName("verbose").setShortName("v").setLongName("verbose"))
        ).processHandler(process -> {
            CommandLine commandLine = process.commandLine();
            repairCatalogue(process, commandLine.getArgumentValue(0));
        }).build(vertx);
    }

    public static Command create(Vertx vertx) {
        return new RepairCatalogueCommand(vertx).command;
    }

    private void repairCatalogue(CommandProcess process, String catalogueId) {
        DCATAPUriRef catalogueSchema = DCATAPUriSchema.applyFor(catalogueId);

        process.write("Start repairing catalogue " + catalogueSchema.getId() + "\n");

        catalogueManager.get(catalogueId)
                .onSuccess(catalogueModel -> {
                    Resource catalogue = catalogueModel.getResource(catalogueSchema.getCatalogueUriRef());

                    List<Resource> datasets = catalogueModel.listObjectsOfProperty(DCAT.dataset).mapWith(RDFNode::asResource).toList();
                    process.write("Found " + datasets.size() + " datasets\n");

                    List<List<Resource>> partitions = Lists.partition(datasets, 1000);
                    Promise<Void> partitionsPromise = Promise.promise();
                    nextPartition(catalogue, partitions, 0, process, partitionsPromise);
                    partitionsPromise.future().onSuccess(v -> {
                        List<String> recordList = catalogueModel.listObjectsOfProperty(DCAT.record)
                                .mapWith(node -> DCATAPUriSchema.parseUriRef(node.asResource().getURI()).getId()).toList();
                        List<String> datasetList = catalogueModel.listObjectsOfProperty(DCAT.dataset)
                                .mapWith(node -> DCATAPUriSchema.parseUriRef(node.asResource().getURI()).getId()).toList();

                        recordList.removeAll(datasetList);
                        recordList.forEach(id -> {
                            Resource record = catalogueModel.getResource(DCATAPUriSchema.applyFor(id).getRecordUriRef());
                            catalogueModel.remove(catalogue, DCAT.record, record);
                        });

                        catalogueManager.set(catalogueId, catalogueModel).onSuccess(s -> {
                            process.write("Repairing catalogue finished\n").end();
                        }).onFailure(cause -> {
                            process.write("Catalogue update failed\n").end();
                        });
                    }).onFailure(cause -> {
                        process.write("Repairing catalogue failed: " + cause.getMessage() + "\n").end();
                    });
                }).onFailure(cause -> {
                    process.write("Can't get catalogue " + catalogueId + ": " + cause.getMessage() + "\n").end();
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
                DCATAPUriRef datasetSchema = DCATAPUriSchema.parseUriRef(dataset.getURI());
                Promise<Void> datasetPromise = Promise.promise();
                futures.add(datasetPromise.future());

                tripleStore.getDatasetManager().existGraph(dataset.getURI()).onSuccess(exists -> {
                    if (exists) {
                        if (!catalogue.hasProperty(DCAT.record, catalogue.getModel().getResource(datasetSchema.getRecordUriRef()))) {
                            catalogue.addProperty(DCAT.record, catalogue.getModel().getResource(datasetSchema.getDatasetUriRef()));
                        }
                    } else {
                        catalogue.getModel().removeAll(catalogue, DCAT.record, catalogue.getModel().getResource(datasetSchema.getRecordUriRef()));
                        catalogue.getModel().removeAll(catalogue, DCAT.dataset, catalogue.getModel().getResource(datasetSchema.getDatasetUriRef()));
                    }
                    datasetPromise.complete();
                }).onFailure(cause -> {
                    process.write("Checking dataset existence failed: " + cause.getMessage() + "\n");
                    datasetPromise.fail(cause);
                });
                // do more here repairing
//                tripleStore.deleteGraph(datasetSchema.getValidationGraphName());
            });
            CompositeFuture.join(new ArrayList<>(futures)).onComplete(cf ->
                    nextPartition(catalogue, partitions, index + 1, process, promise));
        }
    }

}
