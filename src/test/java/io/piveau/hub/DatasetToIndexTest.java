package io.piveau.hub;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.indexing.Indexing;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.JenaUtils;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@DisplayName("Testing indexing")
@ExtendWith(VertxExtension.class)
class DatasetToIndexTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetToIndexTest.class);

    @Test
    @DisplayName("Indexing an example dataset")
    @Timeout(timeUnit = TimeUnit.MINUTES, value = 5)
    void testExampleDataset(Vertx vertx, VertxTestContext testContext) {
        Buffer buffer = vertx.fileSystem().readFileBlocking("example_index_dataset.ttl");
        DatasetHelper.create(buffer.toString(), RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                DatasetHelper helper = ar.result();
                JsonObject result = Indexing.indexingDataset(helper.resource(), "test-catalog", "fr");
                LOGGER.debug(result.encodePrettily());
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Indexing an example catalogue")
    @Timeout(timeUnit = TimeUnit.MINUTES, value = 5)
    void testExampleCatalog(Vertx vertx, VertxTestContext testContext) {
        Buffer buffer = vertx.fileSystem().readFileBlocking("example_catalog.ttl");
        Model model = JenaUtils.read(buffer.getBytes(), "text/turtle");
        Resource catalogue = model.listSubjectsWithProperty(RDF.type, DCAT.Catalog).next();
        JsonObject result = Indexing.indexingCatalogue(catalogue);
        LOGGER.debug(result.encodePrettily());
        testContext.completeNow();
    }

}
