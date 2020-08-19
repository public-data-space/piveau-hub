package io.piveau.hub;

import io.piveau.hub.services.metrics.MetricsService;
import io.piveau.hub.services.metrics.MetricsServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.JenaUtils;
import io.piveau.test.MockTripleStore;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("Testing the metrics service")
@ExtendWith(VertxExtension.class)
class MetricImplServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricImplServiceTest.class);

    private MetricsService metricsService;

    private final String catalogueId = "test-catalog";
    private final String datasetId = "test-dataset";

    private String exampleMetric;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG, MockTripleStore.getDefaultConfig()));

        Future<String> mockFuture = new MockTripleStore()
                .loadGraph("https://piveau.io/id/catalogue/test-catalog", "example_catalog.ttl")
                .loadGraph("https://piveau.io/set/data/test-dataset", "example_dataset.ttl")
                .deploy(vertx);

        Promise<String> metricsPromise = Promise.promise();

        vertx.deployVerticle(MetricsServiceVerticle.class.getName(), options, metricsPromise);

        CompositeFuture.join(mockFuture, metricsPromise.future()).onComplete(ar -> {
            if (ar.succeeded()) {
                metricsService = MetricsService.createProxy(vertx, MetricsService.SERVICE_ADDRESS);
                exampleMetric = vertx.fileSystem().readFileBlocking("example_metric.ttl").toString();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });

    }

    @Test
    @DisplayName("Add a metric")
    void testCreateMetric(Vertx vertx, VertxTestContext testContext) {
        metricsService.putMetric("https://piveau.io/set/data/test-dataset", exampleMetric, RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                assertEquals("created", ar.result());
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Receive a metric")
    void testGetMetric(Vertx vertx, VertxTestContext testContext) {
        metricsService.putMetric("https://piveau.io/set/data/test-dataset", exampleMetric, RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                metricsService.getMetric(datasetId, catalogueId, RDFMimeTypes.TURTLE, ar2 -> {

                    if (ar2.succeeded()) {
                        String model = ar2.result();
                        assertNotNull(model);
                        Model jenaModel = JenaUtils.read(model.getBytes(), RDFMimeTypes.TURTLE);
                        assertNotNull(jenaModel);
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
    @DisplayName("Delete a metric")
    @Timeout(timeUnit = TimeUnit.MINUTES, value = 5)
    void testDeleteMetric(Vertx vertx, VertxTestContext testContext) {
        metricsService.putMetric("https://piveau.io/set/data/test-dataset", exampleMetric, RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                metricsService.deleteMetric(datasetId, catalogueId, handler -> {
                    if (handler.succeeded()) {
                        metricsService.getMetric(datasetId, catalogueId, RDFMimeTypes.TURTLE, ar2 -> {
                            if (ar2.failed()) {
                                testContext.completeNow();
                            } else {
                                testContext.failNow(new Throwable("Metric still exist"));
                            }
                        });
                    } else {
                        testContext.failNow(handler.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Add a measurement to a metric")
    void testPutMeasurement(Vertx vertx, VertxTestContext testContext) {
        String measurement = vertx.fileSystem().readFileBlocking("example_measurement.ttl").toString();

        metricsService.putMetric("https://piveau.io/set/data/test-dataset", exampleMetric, RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                metricsService.putMeasurement(measurement, RDFMimeTypes.TURTLE, mr -> {
                    if (mr.succeeded()) {
                        metricsService.getMetric(datasetId, catalogueId, RDFMimeTypes.TURTLE, gr -> {
                            if (gr.succeeded()) {
                                LOGGER.debug(gr.result());
                                testContext.completeNow();
                            } else {
                                testContext.failNow(gr.cause());
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

}
