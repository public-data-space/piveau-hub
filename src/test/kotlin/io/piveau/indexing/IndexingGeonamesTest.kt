package io.piveau.indexing;

import io.piveau.vocabularies.IanaTypeFactory
import io.piveau.vocabularies.initRemotes
import io.piveau.vocabularies.readTurtleResource
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.DCAT
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException

@DisplayName("indexing test")
@ExtendWith(VertxExtension::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexingGeonamesTest {
    val log = LoggerFactory.getLogger("IndexingTest")

    val model = ModelFactory.createDefaultModel().apply {
        readTurtleResource("example_dataset_geonames.ttl")
    }

    @BeforeAll
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        testContext.completeNow()
    }

    @Test
    fun `example indexing dataset for geonames`(vertx: Vertx, testContext: VertxTestContext) {
        initRemotes(vertx)

        val res : Resource = model.getResource("https://piveau.io/set/data/test-dataset")
        val obj : JsonObject = indexingCatalogue(res)

        val country = obj.getJsonObject("country")

        if (country != null && country.getString("title").equals("Germany") && country.getString("id").equals("DE")) testContext.completeNow()
        else testContext.failNow(IllegalStateException("Resolving Geonames URI was not correct!"))
    }

}
