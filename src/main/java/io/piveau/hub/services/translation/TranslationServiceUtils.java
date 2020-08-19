package io.piveau.hub.services.translation;

import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.dcatap.LanguageTag;
import io.vertx.core.json.JsonObject;
import kotlin.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;

import java.util.List;
import java.util.Map;

public class TranslationServiceUtils {

    public static Map<String, String> codes = Map.ofEntries(
            Map.entry("bg", "http://publications.europa.eu/resource/authority/language/BUL"),
            Map.entry("hr", "http://publications.europa.eu/resource/authority/language/HRV"),
            Map.entry("cs", "http://publications.europa.eu/resource/authority/language/CES"),
            Map.entry("da", "http://publications.europa.eu/resource/authority/language/DAN"),
            Map.entry("nl", "http://publications.europa.eu/resource/authority/language/NLD"),
            Map.entry("et", "http://publications.europa.eu/resource/authority/language/EST"),
            Map.entry("fi", "http://publications.europa.eu/resource/authority/language/FIN"),
            Map.entry("fr", "http://publications.europa.eu/resource/authority/language/FRA"),
            Map.entry("el", "http://publications.europa.eu/resource/authority/language/ELL"),
            Map.entry("hu", "http://publications.europa.eu/resource/authority/language/HUN"),
            Map.entry("ga", "http://publications.europa.eu/resource/authority/language/GLE"),
            Map.entry("it", "http://publications.europa.eu/resource/authority/language/ITA"),
            Map.entry("lv", "http://publications.europa.eu/resource/authority/language/LAV"),
            Map.entry("lt", "http://publications.europa.eu/resource/authority/language/LIT"),
            Map.entry("mt", "http://publications.europa.eu/resource/authority/language/MLT"),
            Map.entry("pl", "http://publications.europa.eu/resource/authority/language/POL"),
            Map.entry("pt", "http://publications.europa.eu/resource/authority/language/POR"),
            Map.entry("ro", "http://publications.europa.eu/resource/authority/language/RON"),
            Map.entry("sk", "http://publications.europa.eu/resource/authority/language/SLK"),
            Map.entry("sl", "http://publications.europa.eu/resource/authority/language/SLV"),
            Map.entry("es", "http://publications.europa.eu/resource/authority/language/SPA"),
            Map.entry("sv", "http://publications.europa.eu/resource/authority/language/SWE"),
            Map.entry("nb", "http://publications.europa.eu/resource/authority/language/NOB"),
            Map.entry("de", "http://publications.europa.eu/resource/authority/language/DEU"),
            Map.entry("en", "http://publications.europa.eu/resource/authority/language/ENG"),
            Map.entry("me", "http://publications.europa.eu/resource/authority/language/CNR")
    );

    public static String getOriginalText(Resource resource, Property text, String language) {
        Statement statement = resource.getProperty(text, language);
        if (statement == null) {
            statement = resource.getProperty(text, "");
        }
        return statement != null ? statement.getString() : null;
    }

    public static List<String> getAvailableLanguages(Resource resource, String defaultLang) {
        return resource.listProperties(DCTerms.title)
                .filterKeep(statement -> statement.getObject().isLiteral())
                .mapWith(statement -> LanguageTag.parseLangTag(statement.getLanguage(), defaultLang))
                .filterKeep(triple -> !triple.getFirst())
                .mapWith(Triple::getSecond)
                .toList();
    }

    public static String getOriginalLanguage(Resource resource, String defaultLang) {
        List<String> available = getAvailableLanguages(resource, defaultLang);
        return available.isEmpty() || available.contains(defaultLang) ? defaultLang : available.get(0);
    }

    public static JsonObject getDataDict(Resource resource, String originalLanguage) {
        JsonObject dataDict = new JsonObject();
        String title = getOriginalText(resource, DCTerms.title, originalLanguage);
        if (title != null) {
            dataDict.put("title", title);
        }
        if (resource.hasProperty(DCTerms.description)) {
            String description = getOriginalText(resource, DCTerms.description, originalLanguage);
            if (description != null) {
                dataDict.put("description", description);
            }
        }

        resource.listProperties(DCAT.distribution)
                .filterKeep(statement -> statement.getObject().isURIResource())
                .mapWith(Statement::getResource)
                .forEachRemaining(distribution -> {
                    String distId = DCATAPUriSchema.parseUriRef(distribution.getURI()).getId();
                    if (distribution.hasProperty(DCTerms.title)) {
                        String titl = getOriginalText(distribution, DCTerms.title, originalLanguage);
                        if (title != null) {
                            dataDict.put(distId + "titl", titl);
                        }
                    }
                    if (distribution.hasProperty(DCTerms.description)) {
                        String desc = getOriginalText(distribution, DCTerms.description, originalLanguage);
                        if (desc != null) {
                            dataDict.put(distId + "desc", desc);
                        }
                    }
                });

        return dataDict;
    }

    public static void removeTranslations(Resource resource, Property property) {
        ExtendedIterator<Statement> it = resource.listProperties(property).filterKeep(statement ->
                statement.getObject().isLiteral() && statement.getLiteral().getLanguage().contains("-t0-mtec"));
        while (it.hasNext()) {
            it.removeNext().remove();
        }
    }

    public static String buildLanguageTag(String originalLanguage, String targetLanguage) {
        if (targetLanguage.equals("nb")) {
            targetLanguage = "no";
        }
        return targetLanguage + "-t-" + originalLanguage + "-t0-mtec";
    }

}
