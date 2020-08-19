package io.piveau.hub;

import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.metrics.MetricsService;
import io.piveau.hub.services.metrics.MetricsServiceVerticle;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.JenaUtils;
import io.piveau.test.MockTripleStore;
import io.piveau.dcatap.DCATAPUriSchema;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.http.HttpHeaders;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the datasets service")
@ExtendWith(VertxExtension.class)
class DatasetImplServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetImplServiceTest.class);
    private static String exampleDataset;
    private final String catalogueID = "test-catalog";
    private DatasetsService datasetsService;
    private MetricsService metricsService;


    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG, MockTripleStore.getDefaultConfig())
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", false))
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject().put("enable", false)));

        Checkpoint checkpoint = testContext.checkpoint(4);

        vertx.fileSystem().readFile("example_external_dataset.ttl", readResult -> {
            if (readResult.succeeded()) {
                exampleDataset = readResult.result().toString();
                checkpoint.flag();
            } else {
                testContext.failNow(readResult.cause());
            }
        });

        new MockTripleStore()
                .loadGraph("https://piveau.io/id/catalogue/test-catalog", "example_empty_catalog.ttl")
                .deploy(vertx)
                .onSuccess(v -> checkpoint.flag())
                .onFailure(testContext::failNow);


        vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                datasetsService = DatasetsService.createProxy(vertx, DatasetsService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        vertx.deployVerticle(MetricsServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                metricsService = MetricsService.createProxy(vertx, MetricsService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.eventBus().consumer(IndexService.SERVICE_ADDRESS, message -> {
            message.reply(new JsonObject());
        });
        vertx.eventBus().<JsonObject>consumer(TranslationService.SERVICE_ADDRESS, message -> {
            message.reply(message.body().getJsonObject("helper"));
        });
    }

    @Test
    @DisplayName("Update example dataset")
    void testUpdateExampleDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "update-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.getDataset(datasetID, catalogueID, "text/turtle", gr -> {
                    if (gr.succeeded()) {
                        LOGGER.debug(gr.result().getString("content"));
                        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, pr -> {
                            if (pr.succeeded()) {
                                datasetsService.getDataset(datasetID, catalogueID, "text/turtle", gr2 -> {
                                    if (gr2.succeeded()) {
                                        LOGGER.debug(gr2.result().getString("content"));
                                        testContext.completeNow();
                                    } else {
                                        testContext.failNow(gr2.cause());
                                    }
                                });
                            } else {
                                testContext.failNow(pr.cause());
                            }
                        });
                    } else {
                        testContext.failNow(gr.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }


    @Test
    @DisplayName("Create an example dataset")
    void testCreateExampleDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "create-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                testContext.verify(() -> {
                    assertEquals("created", result.getString("status", ""));
                    String location = result.getString(HttpHeaders.LOCATION);
                    assertNotNull(location);
                    assertEquals(DCATAPUriSchema.applyFor(datasetID).getDatasetUriRef(), location);
                });
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Delete an example dataset (inclusively Dqv)")
    void testDeleteExampleDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                Buffer buffer = vertx.fileSystem().readFileBlocking("example_metric.ttl");
                metricsService.putMetric("https://piveau.io/set/data/delete-test-dataset", buffer.toString(), RDFMimeTypes.TRIG, mr -> {
                    if (mr.succeeded()) {
                        datasetsService.deleteDataset(datasetID, catalogueID, handler -> {
                            if (handler.succeeded()) {
                                datasetsService.getDataset(datasetID, catalogueID, RDFMimeTypes.TURTLE, ar2 -> {
                                    if (ar2.succeeded()) {
                                        JsonObject result = ar2.result();
                                        metricsService.getMetric(datasetID, catalogueID, RDFMimeTypes.TURTLE, ar3 -> {
                                            if (ar3.failed()) {
                                                testContext.verify(() -> {
                                                    assertNotNull(result);
                                                    assertEquals("not found", result.getString("status", ""));
                                                });
                                                testContext.completeNow();
                                            }
                                        });
                                        testContext.completeNow();
                                    } else {
                                        testContext.failNow(ar2.cause());
                                    }
                                });
                            } else {
                                testContext.failNow(handler.cause());
                            }
                        });
                    } else {
                        testContext.failNow(mr.cause());
                    }
                });

            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Receive an example dataset")
    void testGetExampleDataset(Vertx vertx, VertxTestContext testContext) {

        datasetsService.putDataset("get-test-dataset", exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.getDataset("get-test-dataset", catalogueID, RDFMimeTypes.TURTLE, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertEquals("success", result.getString("status", ""));
                            LOGGER.debug(result.encodePrettily());
                            String content = result.getString("content");
                            assertNotNull(content);
                            DatasetHelper.create(content, RDFMimeTypes.TURTLE, modelResult -> {
                                assertTrue(modelResult.succeeded());
                                assertEquals("get-test-dataset", modelResult.result().id());
                            });
                        });
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar2.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    //    @Test
    @DisplayName("Receive an example dataset from normalized id")
    void testGetExampleDatasetNormalizedID(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "test-Get-normalized-Dataset .id";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.getDatasetByNormalizedId(DCATAPUriSchema.applyFor(datasetID).getId(), RDFMimeTypes.TURTLE, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertEquals("success", result.getString("status", ""));
                            LOGGER.debug(result.encodePrettily());
                            String model = result.getString("content");
                            assertNotNull(model);
                            Model jenaModel = JenaUtils.read(model.getBytes(), RDFMimeTypes.TURTLE);
                            assertNotNull(jenaModel);

                            Resource cat = jenaModel.getResource(DCATAPUriSchema.applyFor(datasetID).getDatasetUriRef());
                            assertNotNull(cat);
                            assertNotNull(cat.getProperty(RDF.type));
                            assertEquals(DCAT.Dataset, cat.getProperty(RDF.type).getObject());
                        });
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar2.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Delete a non existing dataset")
    void testDeleteMissingDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-missing-test-dataset";
        datasetsService.deleteDataset(datasetID, catalogueID, ar -> {
            testContext.verify(() -> {
                assertFalse(ar.succeeded());
            });
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Counting one dataset")
    void testCountOneDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-one-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.listDatasets("application/json", catalogueID, 20, 0, true, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getJsonArray("content"));
                            assertFalse(result.getJsonArray("content").isEmpty());
                            assertEquals(1, result.getJsonArray("content").size());
                            assertEquals(datasetID, result.getJsonArray("content").getString(0));
                        });
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar2.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Counting two datasets")
    void testCountTwoDatasetsInOneCat(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-two-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.putDataset(datasetID + "2", exampleDataset, RDFMimeTypes.TURTLE, catalogueID, null, false, putHandler -> {
                    if (putHandler.succeeded()) {
                        datasetsService.listDatasets("application/json", catalogueID, 20, 0, true, ar2 -> {
                            if (ar2.succeeded()) {
                                JsonObject result = ar2.result();
                                testContext.verify(() -> {
                                    assertNotNull(result);
                                    assertEquals("success", result.getString("status", ""));
                                    assertNotNull(result.getJsonArray("content"));
                                    assertFalse(result.getJsonArray("content").isEmpty());
                                    assertEquals(2, result.getJsonArray("content").size());
                                });
                                testContext.completeNow();
                            } else {
                                testContext.failNow(ar2.cause());
                            }
                        });
                    } else {
                        testContext.failNow(putHandler.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    //TODO: create Dataset & check if distribution is renamed
    //TODO: add update dataset with a dataset that has an additional Dist (without dct:idenifier) & dist is correctly renamed
    //TODO: add update dataset with a dataset that has an additional Dist (with dct:idenifier)  & dist is correctly renamed
    //TODO: list from empty catalogue, list from nonexisting catalog, (list sources, list datasets )x(with&, without catalog)

}
