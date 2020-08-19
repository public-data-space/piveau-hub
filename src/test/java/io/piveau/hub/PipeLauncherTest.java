package io.piveau.hub;

import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.Constants;
import io.piveau.pipe.Payload;
import io.piveau.pipe.Pipe;
import io.piveau.pipe.PiveauCluster;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.test.MockTripleStore;
import io.piveau.vocabularies.ConceptSchemes;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the launcher")
@ExtendWith(VertxExtension.class)
class PipeLauncherTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private DatasetsService datasetsService;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {

        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG, MockTripleStore.getDefaultConfig())
                        .put(Constants.ENV_PIVEAU_CLUSTER_CONFIG, "{\"serviceDiscovery\": {\"launcher-test-segment\": {\"endpoints\": {\"http\": {\"address\": \"http://localhost:8098/pipe\"}}}}}")
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", true).put("metricsPipeName", "launcher-test-pipe"))
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject().put("enable", false)));

        ConceptSchemes.initRemotes(vertx, false, true);

        Future<String> mockFuture = new MockTripleStore()
                .loadGraph("https://piveau.io/id/catalogue/test-catalog", "example_catalog.ttl")
                .deploy(vertx);

        PiveauCluster.create(
                vertx,
                new JsonObject()
                        .put("serviceDiscovery", new JsonObject()
                                .put("launcher-test-segment", new JsonObject()
                                        .put("endpoints", new JsonObject()
                                                .put("http", new JsonObject()
                                                        .put("address", "http://localhost:8098/pipe"))))),
                true).onSuccess(cluster -> {
            Promise<String> datasetServicePromise = Promise.promise();
            vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), options, datasetServicePromise);

            CompositeFuture.join(mockFuture, datasetServicePromise.future()).onComplete(ar -> {
                if (ar.succeeded()) {
                    datasetsService = DatasetsService.createProxy(vertx, DatasetsService.SERVICE_ADDRESS);
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });

            vertx.eventBus().consumer(IndexService.SERVICE_ADDRESS, message -> message.reply(new JsonObject()));
            vertx.eventBus().<JsonObject>consumer(TranslationService.SERVICE_ADDRESS, message -> message.reply(message.body().getJsonObject("helper")));
        }).onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("Put dataset and launch test pipe")
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    void testLaunchPipe(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        vertx.createHttpServer().requestHandler(request -> {
            testContext.verify(() -> {
                assertEquals(HttpMethod.POST, request.method());
                assertEquals("/pipe", request.path());
                assertEquals("application/json", request.getHeader("Content-Type"));

                request.bodyHandler(buffer -> {
                    log.debug(buffer.toJsonObject().encodePrettily());
                    Pipe pipe = Json.decodeValue(buffer, Pipe.class);

                    Payload payload = pipe.getBody().getSegments().get(0).getBody().getPayload();
                    assertEquals(RDFMimeTypes.TRIG, payload.getBody().getDataMimeType());
                    assertEquals("launch-test-dataset", payload.getBody().getDataInfo().get("identifier").textValue());
                    assertNotNull(payload.getBody().getData());
                });
            });
            request.response().setStatusCode(202).end();
            checkpoint.flag();
        }).listen(8098);

        datasetsService.putDataset("launch-test-dataset", vertx.fileSystem().readFileBlocking("example_dataset.ttl").toString(), RDFMimeTypes.TURTLE, "test-catalog", null, false, ar -> {
            if (ar.succeeded()) {
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

}
