package io.piveau.hub.shell;

import io.piveau.dcatap.*;
import io.piveau.hub.util.Constants;
import io.piveau.json.ConfigHelper;
import io.piveau.pipe.PipeLauncher;
import io.piveau.pipe.PiveauCluster;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.*;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.shell.command.Command;
import io.vertx.ext.shell.command.CommandBuilder;
import io.vertx.ext.shell.command.CommandProcess;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LaunchCatalogueCommand {

    private final Command command;

    private final CatalogueManager catalogueManager;
    private final DatasetManager datasetManager;
    private final MetricsManager metricsManager;

    private PipeLauncher pipeLauncher;

    private LaunchCatalogueCommand(Vertx vertx) {
        JsonObject config = vertx.getOrCreateContext().config();
        TripleStore tripleStore = new TripleStore(vertx, ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG), null);
        catalogueManager = tripleStore.getCatalogueManager();
        datasetManager = tripleStore.getDatasetManager();
        metricsManager = tripleStore.getMetricsManager();

        JsonObject clusterConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_CLUSTER_CONFIG);

        PiveauCluster.create(vertx, clusterConfig).onSuccess(cluster -> pipeLauncher = cluster.pipeLauncher(vertx));

        command = CommandBuilder.command(
                CLI.create("launch")
                        .addArgument(
                                new Argument()
                                        .setArgName("catalogueId")
                                        .setRequired(true)
                                        .setDescription("The id of the catalogues to launch pipe with."))
                        .addArgument(
                                new Argument()
                                        .setArgName("pipeName")
                                        .setRequired(true)
                                        .setDescription("The name of the pipe to launch."))
                        .addOption(
                                new Option()
                                        .setArgName("pulse")
                                        .setShortName("p")
                                        .setLongName("pulse")
                                        .setDefaultValue("0")
                                        .setDescription("Pulse of pipe feeding in milliseconds"))
                        .addOption(new Option().setFlag(true).setArgName("update").setShortName("u").setLongName("update"))
                        .addOption(new Option().setHelp(true).setFlag(true).setArgName("help").setShortName("h").setLongName("help"))
                        .addOption(new Option().setFlag(true).setArgName("verbose").setShortName("v").setLongName("verbose"))
        ).processHandler(process -> {
            CommandLine commandLine = process.commandLine();
            String pipeName = commandLine.getArgumentValue(1);
            if (pipeLauncher != null && pipeLauncher.isPipeAvailable(pipeName)) {
                launchCatalogue(process, commandLine.getArgumentValue(0), pipeName);
            } else {
                process.write("Pipe " + pipeName + " not available.\n");
                process.end();
            }
        }).build(vertx);
    }

    public static Command create(Vertx vertx) {
        return new LaunchCatalogueCommand(vertx).command;
    }

    private void launchCatalogue(CommandProcess process, String catalogueId, String pipeName) {
        process.write("Start pipe " + pipeName + " for catalogue " + catalogueId + "\n");

        catalogueManager.get(catalogueId).onComplete(ar -> {
            if (ar.succeeded()) {
                Model catalogueModel = ar.result();
                List<Resource> datasets = catalogueModel.listObjectsOfProperty(DCAT.dataset).mapWith(RDFNode::asResource).toList();
                process.write("Found " + datasets.size() + " datasets\n");
                JsonObject dataInfo = new JsonObject();
                dataInfo.put("total", datasets.size());
                dataInfo.put("catalogue", catalogueId);

                DCATAPUriRef catalogueSchema = DCATAPUriSchema.applyFor(catalogueId);
                Resource catalogue = catalogueModel.getResource(catalogueSchema.getCatalogueUriRef());
                dataInfo.put("source", catalogue.getProperty(DCTerms.type).getLiteral().getLexicalForm());

                List<List<Resource>> partitions = Lists.partition(datasets, 1000);
                Promise<Void> partitionsPromise = Promise.promise();
                nextPartition(pipeName, dataInfo, partitions, 0, process, partitionsPromise);
                partitionsPromise.future().onComplete(pr -> {
                    if (pr.succeeded()) {
                        process.write("Catalogue " + catalogueId + " feed into " + pipeName + ".\n");
                    } else {
                        process.write("Launch " + pipeName + " for catalogue failed: " + pr.cause().getMessage() + "\n");
                    }
                    process.end();
                });
            } else {
                process.write("Can't get catalogue " + catalogueId + ": " + ar.cause().getMessage() + "\n");
                process.end();
            }
        });

    }

    private void nextPartition(String pipeName, JsonObject dataInfo, List<List<Resource>> partitions, int index, CommandProcess process, Promise<Void> promise) {
        if (index >= partitions.size()) {
            process.write("No more partitions\n");
            promise.complete();
        } else {

            int pulse = Integer.parseInt(process.commandLine().getOptionValue("pulse"));

            List<Future<Void>> futures = new ArrayList<>();
            List<Resource> partition = partitions.get(index);

            if (pulse > 0) {
                Iterator<Resource> iterator = partition.iterator();
                process.vertx().setPeriodic(pulse, t -> {
                    if (iterator.hasNext()) {
                        futures.add(launchPipe(pipeName, iterator.next(), dataInfo, process));
                    } else {
                        process.vertx().cancelTimer(t);
                        CompositeFuture.join(new ArrayList<>(futures)).onComplete(cf ->
                                nextPartition(pipeName, dataInfo, partitions, index + 1, process, promise)
                        );
                    }
                });
            } else {
                partition.forEach(dataset -> futures.add(launchPipe(pipeName, dataset, dataInfo, process)));
                CompositeFuture.join(new ArrayList<>(futures)).onComplete(cf ->
                        nextPartition(pipeName, dataInfo, partitions, index + 1, process, promise)
                );
            }
        }
    }

    private Future<Void> launchPipe(String pipeName, Resource dataset, JsonObject dataInfo, CommandProcess process) {
        Promise<Void> promise = Promise.promise();
        DCATAPUriRef datasetSchema = DCATAPUriSchema.parseUriRef(dataset.getURI());

        Future<Model> futureDataset = datasetManager.getGraph(dataset.getURI());

        boolean update = process.commandLine().isFlagEnabled("update");
        Future<Model> futureMetric = update ? metricsManager.getGraph(datasetSchema.getMetricsGraphName()) : Future.succeededFuture(ModelFactory.createDefaultModel());

        CompositeFuture.join(futureDataset, futureMetric).onComplete(ar -> {
            if (ar.succeeded()) {
                Dataset jenaDataset = DatasetFactory.create(futureDataset.result());
                if (!futureMetric.result().isEmpty()) {
                    jenaDataset.addNamedModel(datasetSchema.getMetricsGraphName(), futureMetric.result());
                }
                pipeLauncher.runPipeWithData(pipeName,
                        JenaUtils.write(jenaDataset, Lang.TRIG),
                        RDFMimeTypes.TRIG,
                        dataInfo.copy().put("uriRef", dataset.getURI()),
                        new JsonObject().put("validating-shacl", new JsonObject().put("skip", !"dcat-ap".equals(dataInfo.getString("source")))),
                        null).onComplete(lr -> {
                    if (lr.succeeded()) {
                        promise.complete();
                    } else {
                        process.write("Could not launch pipe for " + dataset.getURI() + ": " + lr.cause().getMessage() + "\n");
                        promise.fail(lr.cause());
                    }
                });
            } else {
                process.write("Could not get dataset or its identifier " + dataset.getURI() + ": " + ar.cause().getMessage() + "\n");
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

}
