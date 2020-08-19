package io.piveau.indexing

import io.piveau.vocabularies.readTurtleResource
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@DisplayName("indexing test")
@ExtendWith(VertxExtension::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class
IndexingTest {
    private val log = LoggerFactory.getLogger("IndexingTest")

    private val modelDataset : Model = ModelFactory.createDefaultModel().apply {
        readTurtleResource("example_dataset.ttl")
    }

    private val modelCatalog : Model = ModelFactory.createDefaultModel().apply {
        readTurtleResource("example_catalog.ttl")
    }

    @BeforeAll
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        testContext.completeNow()
    }

    @Test
    fun `test publisher homepage and email with example dataset`(vertx: Vertx, testContext: VertxTestContext) {
        val res : Resource = modelDataset.getResource("https://piveau.io/set/data/test-dataset")
        val obj : JsonObject = indexingDataset(res,"test-catalog", "en")

        val publisher : JsonObject = obj.getJsonObject("publisher")

        assertNotNull(publisher)
        assertEquals("http://www.fokus.fraunhofer.de", publisher.getString("homepage"))
        assertEquals("mailto:info@fokus.fraunhofer.de", publisher.getString("email"))

        log.info("indexing dataset: " + obj.encodePrettily())

        testContext.completeNow()
    }

    @Test
    fun `test publisher homepage and email with example catalog`(vertx: Vertx, testContext: VertxTestContext) {
        val res : Resource = modelCatalog.getResource("https://piveau.io/id/catalogue/test-catalog")
        val obj : JsonObject = indexingCatalogue(res)

        val publisher : JsonObject = obj.getJsonObject("publisher")

        assertNotNull(publisher)
        assertEquals("http://www.fokus.fraunhofer.de", publisher.getString("homepage"))
        assertEquals("mailto:info@fokus.fraunhofer.de", publisher.getString("email"))

        log.info("indexing catalog: " + obj.encodePrettily())

        testContext.completeNow()
    }

}
