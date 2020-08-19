package io.piveau.hub;

import io.piveau.hub.services.distributions.DistributionsService;
import io.piveau.hub.services.distributions.DistributionsServiceVerticle;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.Constants;
import io.piveau.test.MockTripleStore;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Testing the Distributions service")
@ExtendWith(VertxExtension.class)
public class DistributionsServiceImplTest {

    private String mockTripleStoreID;
    private DistributionsService distributionsService;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG, MockTripleStore.getDefaultConfig())
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", false))
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject().put("enable", false)));

/*
        vertx.fileSystem().readFile("example_external_dataset.ttl", readResult -> {
            if(readResult.succeeded()) {
                //exampleDataset = readResult.result().toString();
                checkpoint.flag();
            } else {
                testContext.failNow(readResult.cause());
            }
        });
*/

        vertx.deployVerticle(DistributionsServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                distributionsService = DistributionsService.createProxy(vertx, DistributionsService.SERVICE_ADDRESS);
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.eventBus().consumer(IndexService.SERVICE_ADDRESS, message -> {
            message.reply(new JsonObject());
        });
        vertx.eventBus().<JsonObject>consumer(TranslationService.SERVICE_ADDRESS, message -> {
            message.reply(new JsonObject());
        });
    }


    Future<Void> setupMock(Vertx vertx, String datasetPath) {
        Promise<Void> mockPromise = Promise.promise();

        MockTripleStore mockTripleStore = new MockTripleStore()
                .loadGraph("https://piveau.io/id/catalogue/test-catalog", "distribution/example_catalog.ttl");

        if (datasetPath != null && !datasetPath.isBlank()) {
            mockTripleStore.loadGraph("https://piveau.io/set/data/test-dataset", datasetPath);
        }

        mockTripleStore.deploy(vertx).onSuccess(id -> {
                mockTripleStoreID = id;
                mockPromise.complete();
            }).onFailure(mockPromise::fail);

        return mockPromise.future();
    }

    @Test
    void testGetDistribution(Vertx vertx, VertxTestContext vertxTestContext) {
        setupMock(vertx, "distribution/example_dataset.ttl")
                .onFailure(vertxTestContext::failNow)
                .onSuccess(v -> distributionsService.getDistribution("02e103b1-ffc4-464d-8013-de1c435a5375", "application/ld+json", handler -> {
                    if (handler.succeeded()) {
                        vertxTestContext.verify(() -> {

                            assertTrue(handler.result().getString("status").equalsIgnoreCase("success"));
                            String content = handler.result().getString("content");
                            assertNotNull(content);

                            assertNotNull(new JsonObject(content).getJsonArray("@graph"));

                            JsonObject graph = new JsonObject(content).getJsonArray("@graph").getJsonObject(0);
                            assertNotNull(graph);
                            assertTrue(graph.getString("identifier").equalsIgnoreCase("https://example.com/dataset/1"));
                            assertTrue(graph.getString("@id").equalsIgnoreCase("https://piveau.io/set/distribution/02e103b1-ffc4-464d-8013-de1c435a5375"));
                        });
                        vertxTestContext.completeNow();
                    } else {
                        vertxTestContext.failNow(handler.cause());
                    }
                }));

    }


    @Test
    void testGetDistributionWithID(Vertx vertx, VertxTestContext vertxTestContext) {
        setupMock(vertx, "distribution/example_dataset.ttl")
                .onFailure(vertxTestContext::failNow)
                .onSuccess(v -> distributionsService.getDistributionByIdentifier("https://example.com/dataset/1", "application/ld+json", handler -> {
                    if (handler.succeeded()) {
                        vertxTestContext.verify(() -> {

                            assertTrue(handler.result().getString("status").equalsIgnoreCase("success"));
                            assertNotNull(handler.result().getJsonObject("content"));
                            assertNotNull(handler.result().getJsonObject("content").getJsonArray("@graph"));

                            JsonObject graph = handler.result().getJsonObject("content").getJsonArray("@graph").getJsonObject(0);
                            assertNotNull(graph);
                            assertTrue(graph.getString("identifier").equalsIgnoreCase("https://example.com/dataset/1"));
                            assertTrue(graph.getString("@id").equalsIgnoreCase("https://piveau.io/set/distribution/02e103b1-ffc4-464d-8013-de1c435a5375"));
                        });

                        vertxTestContext.completeNow();
                    } else {
                        vertxTestContext.failNow(handler.cause());
                    }
                }));

    }


    @Test
    void testPostDistribution(Vertx vertx, VertxTestContext vertxTestContext) {


        Buffer distribution = vertx.fileSystem().readFileBlocking("distribution/example_distribution.ttl");

        setupMock(vertx, "distribution/example_dataset.ttl")
                .onFailure(vertxTestContext::failNow)
                .onSuccess(v -> distributionsService.postDistribution(distribution.toString(), "test-dataset", "text/turtle", "test-catalog", handler -> {
                    if (handler.succeeded()) {
                        vertxTestContext.verify(() -> {

                            assertTrue(handler.result().getString("status").equalsIgnoreCase("success"));
                            assertNotNull(handler.result().getString("content"));

                            String loc = handler.result().getString("content");
                            System.out.println(loc);


                        });
                        vertxTestContext.completeNow();
                    } else {
                        vertxTestContext.failNow(handler.cause());
                    }
                }));

    }

    @Test
    void testPutDistribution(Vertx vertx, VertxTestContext vertxTestContext) {


        Buffer distribution = vertx.fileSystem().readFileBlocking("distribution/example_distribution_update.ttl");

        setupMock(vertx, "distribution/example_dataset.ttl")
                .onFailure(vertxTestContext::failNow)
                .onSuccess(v -> distributionsService.putDistribution(distribution.toString(), "02e103b1-ffc4-464d-8013-de1c435a5375", "text/turtle", handler -> {
                    if (handler.succeeded()) {
                        vertxTestContext.verify(() -> {
                            assertTrue(handler.result().getString("status").equalsIgnoreCase("success"));
                        });

                        distributionsService.getDistribution("02e103b1-ffc4-464d-8013-de1c435a5375", "application/ld+json", h -> {
                            if (h.succeeded()) {
                                vertxTestContext.verify(() -> {

                                    assertTrue(h.result().getString("status").equalsIgnoreCase("success"));
                                    String content = h.result().getString("content");
                                    assertNotNull(content);
                                    assertNotNull(new JsonObject(content).getJsonArray("@graph"));

                                    JsonObject graph = new JsonObject(content).getJsonArray("@graph").getJsonObject(0);
                                    assertNotNull(graph);
                                    assertTrue(graph.getString("identifier").equalsIgnoreCase("https://example.com/dataset/1"));
                                    assertTrue(graph.getString("@id").equalsIgnoreCase("https://piveau.io/set/distribution/02e103b1-ffc4-464d-8013-de1c435a5375"));
                                });
                                vertxTestContext.completeNow();
                            } else {
                                vertxTestContext.failNow(h.cause());
                            }
                        });
                    } else {
                        vertxTestContext.failNow(handler.cause());
                    }
                }));
    }

    @Test
    void testPutDistributionWithID(Vertx vertx, VertxTestContext vertxTestContext) {


        Buffer distribution = vertx.fileSystem().readFileBlocking("distribution/example_distribution_update.ttl");

        setupMock(vertx, "distribution/example_dataset.ttl")
                .onFailure(vertxTestContext::failNow)
                .onSuccess(v -> distributionsService.putDistributionWithIdentifier(distribution.toString(), "https://example.com/dataset/1", "text/turtle", handler -> {
                    if (handler.succeeded()) {
                        vertxTestContext.verify(() -> {
                            assertTrue(handler.result().getString("status").equalsIgnoreCase("success"));
                        });

                        distributionsService.getDistributionByIdentifier("https://example.com/dataset/1", "application/ld+json", h -> {
                            if (h.succeeded()) {
                                vertxTestContext.verify(() -> {

                                    assertTrue(h.result().getString("status").equalsIgnoreCase("success"));
                                    assertNotNull(h.result().getJsonObject("content"));
                                    assertNotNull(h.result().getJsonObject("content").getJsonArray("@graph"));

                                    JsonObject graph = h.result().getJsonObject("content").getJsonArray("@graph").getJsonObject(0);
                                    assertNotNull(graph);
                                    assertTrue(graph.getString("identifier").equalsIgnoreCase("https://example.com/dataset/1"));
                                    assertTrue(graph.getString("@id").equalsIgnoreCase("https://piveau.io/set/distribution/02e103b1-ffc4-464d-8013-de1c435a5375"));
                                });
                                vertxTestContext.completeNow();
                            } else {
                                vertxTestContext.failNow(h.cause());
                            }
                        });
                    } else {
                        vertxTestContext.failNow(handler.cause());
                    }
                }));
    }
}

//TODO: what if there are two datasets whose distributions have the same identifier?