package io.piveau.hub;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.services.translation.TranslationServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.test.MockTripleStore;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.http.HttpStatus;
import org.apache.jena.vocabulary.DCTerms;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("Testing the Translation Service verticle")
@ExtendWith(VertxExtension.class)
class TranslationServiceImplTest {

    TranslationService translationService;
    DatasetHelper datasetHelper;
    DatasetHelper datasetHelperOld;
    String exampleDataset;


    private void error500(RoutingContext routingContext) {
        routingContext.response().setStatusCode(500).end();
    }

    private void error404(RoutingContext routingContext) {
        routingContext.response().setStatusCode(404).end();
    }

    private void success200(RoutingContext routingContext) {
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end();
    }

    private void success200WithCheckpoint(RoutingContext routingContext, Checkpoint checkpoint) {
        checkpoint.flag();
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end();
    }

    private void echo200(RoutingContext routingContext) {
        routingContext.response().setStatusCode(HttpStatus.SC_OK).end(routingContext.getBodyAsString());
    }


    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG, MockTripleStore.getDefaultConfig())
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", false))
                        .put(Constants.ENV_PIVEAU_HUB_API_KEY, "apikey")
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject()
                                .put("enable", true)
                                .put("accepted_languages", new JsonArray(Arrays.asList("en", "bg", "hr", "cs", "da", "nl", "et", "fi", "fr", "el",
                                        "hu", "ga", "it", "lv", "lt", "mt", "pl", "pt", "ro", "sk", "sl", "es", "sv", "nb", "de")))
                                .put("callback_url", "http://localhost:9099/callback").put("translation_service_url", "http://localhost:9099/")));


        Checkpoint checkpoint = testContext.checkpoint(3);


        vertx.fileSystem().readFile("example_dataset.ttl", readResult -> {
            if(readResult.succeeded()) {
                exampleDataset = readResult.result().toString();
                DatasetHelper.create(exampleDataset, RDFMimeTypes.TURTLE, h -> {
                    if(h.succeeded()) {
                        datasetHelper = h.result();
                        datasetHelper.sourceLang("de");
                        assertNotNull(datasetHelper.id());
                        datasetHelperOld = new DatasetHelper(datasetHelper);
                        checkpoint.flag();
                    } else
                        testContext.failNow(h.cause());
                });

            } else {
                testContext.failNow(readResult.cause());
            }
        });

        new MockTripleStore()
                .loadGraph("https://piveau.io/id/catalogue/test-catalog", "example_empty_catalog.ttl")
                .deploy(vertx)
                .onSuccess(v -> checkpoint.flag())
                .onFailure(testContext::failNow);


        vertx.deployVerticle(TranslationServiceVerticle.class.getName(), options, ar -> {
            if(ar.succeeded()) {
                translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.eventBus().consumer(IndexService.SERVICE_ADDRESS, message -> {
            message.reply(new JsonObject());
        });

    }

    @Test
    @DisplayName("translate nothing")
    void testTranslateNothing(Vertx vertx, VertxTestContext testContext) {


        Router router = Router.router(vertx);
        router.route("/callback").handler(this::error500);
        // when something is sent to the translation server, return an error
        router.route().handler(this::error404);

        vertx.createHttpServer().requestHandler(router).listen(9099, handler -> {
            if(handler.failed())
                testContext.failNow(handler.cause());
            else {
                translationService.initializeTranslationProcess(datasetHelper, datasetHelperOld, ar -> {
                    if(ar.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
            }
        });
    }

    @Test
    @DisplayName("send to translation server because title is different")
    void testTranslateTitle(Vertx vertx, VertxTestContext testContext) {

        //change title so that new
        datasetHelperOld.resource().getProperty(DCTerms.title).changeObject("this is a new title");

        Checkpoint checkpoint = testContext.checkpoint(2);
        Router router = Router.router(vertx);
        router.route("/callback").handler(this::error500);
        // when something is sent to the translation server, return success and check an checkpoint
        router.route().handler(rc -> success200WithCheckpoint(rc, checkpoint));

        vertx.createHttpServer().requestHandler(router).listen(9099, handler -> {
            if(handler.failed())
                testContext.failNow(handler.cause());
            else {
                translationService.initializeTranslationProcess(datasetHelper, datasetHelperOld, ar -> {
                    if(ar.succeeded()) {
                        checkpoint.flag();

                    } else {
                        testContext.failNow(ar.cause());

                    }
                });
            }
        });
    }
    @Test
    @DisplayName("only new helper")
    void testNoOldHelper(Vertx vertx, VertxTestContext testContext) {

        //change title so that new

        Checkpoint checkpoint = testContext.checkpoint(2);
        Router router = Router.router(vertx);
        router.route("/callback").handler(this::error500);
        // when something is sent to the translation server, return success and check an checkpoint
        router.route().handler(rc -> success200WithCheckpoint(rc, checkpoint));

        vertx.createHttpServer().requestHandler(router).listen(9099, handler -> {
            if(handler.failed())
                testContext.failNow(handler.cause());
            else {
                translationService.initializeTranslationProcess(datasetHelper, null, ar -> {
                    if(ar.succeeded()) {
                        checkpoint.flag();

                    } else {
                        testContext.failNow(ar.cause());

                    }
                });
            }
        });
    }
}
