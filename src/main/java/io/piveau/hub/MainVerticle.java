package io.piveau.hub;

import io.piveau.hub.handler.*;
import io.piveau.hub.services.DatasetHelperMessageCodec;
import io.piveau.hub.services.catalogues.CataloguesService;
import io.piveau.hub.services.catalogues.CataloguesServiceVerticle;
import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.services.distributions.DistributionsService;
import io.piveau.hub.services.distributions.DistributionsServiceVerticle;
import io.piveau.hub.services.index.IndexServiceVerticle;
import io.piveau.hub.services.metrics.MetricsService;
import io.piveau.hub.services.metrics.MetricsServiceVerticle;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.services.translation.TranslationServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.shell.ShellVerticle;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.pipe.PiveauCluster;
import io.piveau.vocabularies.ConceptSchemes;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * This is the main entry point of the application
 */
public class MainVerticle extends AbstractVerticle {

    private CatalogueHandler catalogueHandler;
    private AuthenticationHandler authenticationHandler;
    private AuthorizationHandler authorizationHandler;

    private DatasetHandler datasetHandler;
    private MetricHandler metricHandler;
    private DistributionHandler distributionHandler;

    private TranslationServiceHandler translationServiceHandler;

    /**
     * Composes all function for starting the Main Verticle
     */
    @Override
    public void start(Promise<Void> startPromise) {
        PiveauLoggerFactory.getLogger(getClass()).info("Starting piveau hub...");
        loadConfig()
                /*
                .compose(config -> {
                    Promise<JsonObject> promise = Promise.promise();
                    PiveauCluster.init(vertx, ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_CLUSTER_CONFIG)).onComplete(ar -> {
                       if (ar.succeeded()) {
                            promise.complete(config);
                       } else {
                           promise.fail(ar.cause());
                       }
                    });
                    return promise.future();
                })
                 */
                .compose(this::bootstrapVerticles)
                .compose(this::startServer)
                .onComplete(startPromise);
    }

    /**
     * Starts the HTTP Server based on the OpenAPI Specification
     * ToDo Check the status of depended services here (e.g. search, triplestore, validation)
     *
     * @return
     */
    private Future<Void> startServer(JsonObject config) {
        vertx.eventBus().registerDefaultCodec(DatasetHelper.class, new DatasetHelperMessageCodec());

        PiveauLogger.setBaseUri(config.getString(Constants.ENV_PIVEAU_HUB_BASE_URI, "https://io.piveau/"));
        Promise<Void> promise = Promise.promise();
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

        ConceptSchemes.initRemotes(vertx, config.getBoolean(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES_FETCH, false), false);

        DCATAPUriSchema.INSTANCE.setConfig(new JsonObject().put("baseUri", config.getString(Constants.ENV_PIVEAU_HUB_BASE_URI, "https://io.piveau/")));
        Integer port = config.getInteger(Constants.ENV_PIVEAU_HUB_SERVICE_PORT);
        String apiKey = config.getString(Constants.ENV_PIVEAU_HUB_API_KEY);

        JsonObject authorizationProcessData = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA);

        String greeting = config.getString("greeting");

        if (apiKey == null && authorizationProcessData.isEmpty()) {
            promise.fail("No form of authorization given. Either an API Key or RTP token authorization information must be given.");
            return promise.future();
        } else if (apiKey == null && !authorizationProcessData.isEmpty()) {
            LOGGER.info(Constants.ENV_PIVEAU_HUB_API_KEY + " is missing in the config file. But RTP token authorization information is given, so all should be well.");
            //future.fail(Constants.ENV_PIVEAU_HUB_API_KEY + " is missing in the config file. Please specify it. But RTP token authorization information is given, so all should be well.");
            //return future;
        } else if (apiKey != null && authorizationProcessData.isEmpty()) {
            LOGGER.info(Constants.ENV_PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA + " is missing in the config file. Please specify it. But an API key is given which is okay but it is outdated (though still supported).");
            //future.fail(Constants.ENV_PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA + " is missing in the config file. Please specify it. But an API key is given which is okay but it is outdated (though still supported).");
        }

        // Loads the OpenAPI specification and creates the routes
        OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml", handler -> {
            if (handler.succeeded()) {
                OpenAPI3RouterFactory routerFactory = handler.result();
                RouterFactoryOptions options = new RouterFactoryOptions().setMountNotImplementedHandler(true).setOperationModelKey("assd");
                routerFactory.setOptions(options);

                JsonArray corsDomains = ConfigHelper.forConfig(config).forceJsonArray(Constants.ENV_PIVEAU_HUB_CORS_DOMAINS);
                if (!corsDomains.isEmpty()) {

                    Set<String> allowedHeaders = new HashSet<>();
                    allowedHeaders.add("x-requested-with");
                    allowedHeaders.add("Access-Control-Allow-Origin");
                    allowedHeaders.add("origin");
                    allowedHeaders.add("Content-Type");
                    allowedHeaders.add("accept");
                    allowedHeaders.add("Authorization");

                    Set<HttpMethod> allowedMethods = new HashSet<>();
                    allowedMethods.add(HttpMethod.GET);
                    allowedMethods.add(HttpMethod.POST);
                    allowedMethods.add(HttpMethod.OPTIONS);
                    allowedMethods.add(HttpMethod.DELETE);
                    allowedMethods.add(HttpMethod.PATCH);
                    allowedMethods.add(HttpMethod.PUT);

                    ArrayList<String> corsArray = new ArrayList<>();
                    for (int i = 0; i < corsDomains.size(); i++) {
                        //convert into normal array and escape dots for regex compatiility
                        corsArray.add(corsDomains.getString(i).replace(".", "\\."));
                    }

                    //"^(https?:\\/\\/(?:.+\\.)?(?:fokus\\.fraunhofer\\.de|localhost)(?::\\d{1,5})?)$"
                    String corsString = "^(https?:\\/\\/(?:.+\\.)?(?:" + String.join("|", corsArray) + ")(?::\\d{1,5})?)$";
                    routerFactory.addGlobalHandler(CorsHandler.create(corsString).allowedHeaders(allowedHeaders).allowedMethods(allowedMethods).allowCredentials(true));
                }

                // handler for authentication
                authenticationHandler = new AuthenticationHandler(authorizationProcessData.getString("publicKey"), apiKey, authorizationProcessData.getString("clientID"));
                routerFactory.addSecurityHandler("Authenticate", authenticationHandler::handleAuthentication);

                //handler for authorization
                authorizationHandler = new AuthorizationHandler(authorizationProcessData, config.getString(Constants.ENV_PIVEAU_HUB_BASE_URI, "https://io.piveau/"));
                routerFactory.addSecurityHandler("Authorize", authorizationHandler::handleAuthorization);

                routerFactory.addHandlerByOperationId("listDatasets", datasetHandler::handleListDatasets);
                routerFactory.addHandlerByOperationId("postDataset", datasetHandler::handlePostDataset);
                routerFactory.addHandlerByOperationId("putDataset", datasetHandler::handlePutDataset);
                routerFactory.addHandlerByOperationId("putDatasetQuery", datasetHandler::handlePutDataset);
                routerFactory.addHandlerByOperationId("getDataset", datasetHandler::handleGetDataset);
                routerFactory.addHandlerByOperationId("deleteDataset", datasetHandler::handleDeleteDataset);
                routerFactory.addHandlerByOperationId("deleteDatasetQuery", datasetHandler::handleDeleteDataset);
                routerFactory.addHandlerByOperationId("indexDataset", datasetHandler::handleIndexDataset);

                routerFactory.addHandlerByOperationId("getRecord", datasetHandler::handleGetRecord);


                routerFactory.addHandlerByOperationId("listCatalogues", catalogueHandler::handleListCatalogues);
                //routerFactory.addHandlerByOperationId("postCatalogue", catalogueHandler::handlePostCatalog);
                routerFactory.addHandlerByOperationId("putCatalogue", catalogueHandler::handlePutCatalogue);
                routerFactory.addHandlerByOperationId("getCatalogue", catalogueHandler::handleGetCatalogue);
                routerFactory.addHandlerByOperationId("deleteCatalogue", catalogueHandler::handleDeleteCatalogue);

                routerFactory.addHandlerByOperationId("getDistribution", distributionHandler::handleGetDistribution);
                routerFactory.addHandlerByOperationId("postDistribution", distributionHandler::handlePostDistribution);
                routerFactory.addHandlerByOperationId("deleteDistribution", distributionHandler::handleDeleteDistribution);
                routerFactory.addHandlerByOperationId("putDistribution", distributionHandler::handlePutDistribution);

                routerFactory.addHandlerByOperationId("getMetric", metricHandler::handleGetMetric);
                routerFactory.addHandlerByOperationId("deleteMetric", metricHandler::handleDeleteMetric);
                routerFactory.addHandlerByOperationId("deleteMetricQuery", metricHandler::handleDeleteMetric);
                routerFactory.addHandlerByOperationId("putMetric", metricHandler::handlePutMetric);
                routerFactory.addHandlerByOperationId("putMetricQuery", metricHandler::handlePutMetric);
                routerFactory.addHandlerByOperationId("putMeasurement", metricHandler::handlePutMeasurement);


                routerFactory.addHandlerByOperationId("postTranslationRequest", translationServiceHandler::handlePostTranslationRequest);
                routerFactory.addHandlerByOperationId("postTranslation", translationServiceHandler::handlePostTranslation);

                Router router = routerFactory.getRouter();

                router.route("/*").handler(StaticHandler.create());
                router.route("/info").handler(context -> healthHandler(context, greeting));

                HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(port));
                server.requestHandler(router).listen();

                LOGGER.info("Successfully launched server on port [{}]", port);

                promise.complete();
            } else {
                // Something went wrong during router factory initialization
                LOGGER.error("Failed to start server at [{}]: {}", port, handler.cause());
                promise.fail(handler.cause());
            }
        });

        return promise.future();
    }

    /**
     * Loads the configuration file
     *
     * @return Configuration Object
     */
    private Future<JsonObject> loadConfig() {
        ConfigStoreOptions envStoreOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray()
                        .add(Constants.ENV_PIVEAU_HUB_SERVICE_PORT)
                        .add(Constants.ENV_PIVEAU_HUB_API_KEY)
                        .add(Constants.ENV_PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA)
                        .add(Constants.ENV_PIVEAU_HUB_BASE_URI)
                        .add(Constants.ENV_PIVEAU_HUB_VALIDATOR)
                        .add(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_SERVICE)
                        .add(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES)
                        .add(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES_FETCH)
                        .add(Constants.ENV_PIVEAU_TRANSLATION_SERVICE)
                        .add(Constants.ENV_PIVEAU_DATA_UPLOAD)
                        .add(Constants.ENV_PIVEAU_CLUSTER_CONFIG)
                        .add(Constants.ENV_PIVEAU_HUB_ELASTICSEARCH_ADDRESS)
                        .add(Constants.ENV_PIVEAU_HUB_CORS_DOMAINS)
                        .add("PIVEAU_HUB_SHELL_CONFIG")
                ));

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setConfig(new JsonObject().put("path", "conf/config.json")); // ToDo What is right here?

        ConfigRetriever retriever = ConfigRetriever
                .create(vertx, new ConfigRetrieverOptions()
                        .addStore(fileStoreOptions)
                        .addStore(envStoreOptions));
        Promise<JsonObject> promise = Promise.promise();
        retriever.getConfig(promise);
        return promise.future();
    }

    /**
     * Bootstraps all Verticles
     *
     * @return future
     */
    private Future<JsonObject> bootstrapVerticles(JsonObject config) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());
        LOGGER.info(config.encodePrettily());

        Promise<JsonObject> promise = Promise.promise();

        PiveauCluster.create(
                vertx,
                ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_CLUSTER_CONFIG)).onComplete(cr -> {

            DeploymentOptions options = new DeploymentOptions().setConfig(config).setWorker(true);

            DeploymentOptions shellOptions = new DeploymentOptions().setConfig(config);
            Promise<String> shellPromise = Promise.promise();
            vertx.deployVerticle(ShellVerticle.class.getName(), shellOptions, shellPromise);

            Promise<String> indexPromise = Promise.promise();
            vertx.deployVerticle(IndexServiceVerticle.class.getName(), options, indexPromise);

            Promise<String> datasetsPromise = Promise.promise();
            vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), new DeploymentOptions().setConfig(config).setWorker(true), datasetsPromise);

            Promise<String> distributionsPromise = Promise.promise();
            vertx.deployVerticle(DistributionsServiceVerticle.class.getName(), options, distributionsPromise);

            Promise<String> metricPromise = Promise.promise();
            vertx.deployVerticle(MetricsServiceVerticle.class.getName(), options, metricPromise);

            Promise<String> catalogsPromise = Promise.promise();
            vertx.deployVerticle(CataloguesServiceVerticle.class.getName(), options, catalogsPromise);

            Promise<String> translationSevicePromise = Promise.promise();
            vertx.deployVerticle(TranslationServiceVerticle.class.getName(), options, translationSevicePromise);

            CompositeFuture.all(Arrays.asList(
                    shellPromise.future(),
                    indexPromise.future(),
                    datasetsPromise.future(),
                    distributionsPromise.future(),
                    metricPromise.future(),
                    catalogsPromise.future(),
                    translationSevicePromise.future())).onComplete(ar -> {
                if (ar.succeeded()) {
                    datasetHandler = new DatasetHandler(vertx, DatasetsService.SERVICE_ADDRESS);
                    metricHandler = new MetricHandler(vertx, MetricsService.SERVICE_ADDRESS);
                    distributionHandler = new DistributionHandler(vertx, DistributionsService.SERVICE_ADDRESS);
                    catalogueHandler = new CatalogueHandler(vertx, CataloguesService.SERVICE_ADDRESS);
                    translationServiceHandler = new TranslationServiceHandler(vertx, TranslationService.SERVICE_ADDRESS);
                    promise.complete(config);
                } else {
                    promise.fail(ar.cause());
                }
            });

        });

        return promise.future();
    }

    /**
     * Creates the health and info endpoint
     *
     * @param context
     */
    private void healthHandler(RoutingContext context, String greeting) {
        JsonObject response = new JsonObject();
        response.put("service", "piveau hub");
        response.put("message", greeting);
        response.put("version", "0.0.1");
        response.put("status", "ok");
        context.response().setStatusCode(200);
        context.response().putHeader("Content-Type", "application/json");
        context.response().end(response.encode());
    }

    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }

}
