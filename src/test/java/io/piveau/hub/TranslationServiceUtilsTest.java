package io.piveau.hub;

import io.piveau.hub.services.translation.TranslationServiceUtils;
import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the translation service")
class TranslationServiceUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(TranslationServiceUtilsTest.class);

    private final Resource test = ModelFactory.createDefaultModel().createResource("urn:test", DCAT.Dataset);

    @BeforeEach
    void setUp() {
        test.addLiteral(DCTerms.title, "This is the original english title");
        test.addLiteral(DCTerms.title, test.getModel().createLiteral("Dies ist der deutsche Titel", "de"));
        test.addLiteral(DCTerms.title, test.getModel().createLiteral("C'est la vie", "fr-t-en-t0-mtec"));
        test.addLiteral(DCTerms.description, test.getModel().createLiteral("Eine kleine deutsche Beschreibung", "de"));
        test.addProperty(DCAT.distribution, test.getModel().createResource("urn:dist", DCAT.Distribution));
        test.getModel().getResource("urn:dist").addLiteral(DCTerms.description, test.getModel().createLiteral("Deutsche Distribution Beschreibung", "de"));
    }

    @Test
    @DisplayName("Get available languages")
    void testGetAvailableLanguages() {
        List<String> languages = TranslationServiceUtils.getAvailableLanguages(test, "en");
        assertEquals(List.of("de", "en"), languages);
    }

    @Test
    @DisplayName("Get translation languages")
    void testGetTranslationLanguages() {
        List<String> available = TranslationServiceUtils.getAvailableLanguages(test, "en");
        List<String> acceptable = new ArrayList<>(List.of("en", "de", "fr", "no"));

        acceptable.removeAll(available);
        assertEquals(List.of("fr", "no"), acceptable);
    }

    @Test
    @DisplayName("Get original language")
    void testGetOriginalLanguage() {
        String lang = TranslationServiceUtils.getOriginalLanguage(test, "en");
        assertEquals("en", lang);
        lang = TranslationServiceUtils.getOriginalLanguage(test, "de");
        assertEquals("de", lang);
    }

    @Test
    @DisplayName("Get original text")
    void testGetOriginalText() {
        String english = TranslationServiceUtils.getOriginalText(test, DCTerms.title, "en");
        assertEquals("This is the original english title", english);
        String german = TranslationServiceUtils.getOriginalText(test, DCTerms.title, "de");
        assertEquals("Dies ist der deutsche Titel", german);
    }

    @Test
    @DisplayName("Get data dictionary")
    void testGetDataDict() {
        JsonObject dict = TranslationServiceUtils.getDataDict(test, "en");
        log.debug(dict.encodePrettily());
        assertEquals("{\"title\":\"This is the original english title\"}", dict.encode());
    }

}
