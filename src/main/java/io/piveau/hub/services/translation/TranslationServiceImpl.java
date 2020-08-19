package io.piveau.hub.services.translation;

import io.piveau.dcatap.DCATAPUriRef;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.dcatap.DatasetManager;
import io.piveau.dcatap.TripleStore;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.Constants;
import io.piveau.indexing.Indexing;
import io.piveau.json.ConfigHelper;
import io.piveau.utils.Piveau;
import io.piveau.utils.PiveauContext;
import io.piveau.vocabularies.Languages;
import io.piveau.vocabularies.vocabulary.EDP;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static io.piveau.hub.services.translation.TranslationServiceUtils.*;

public class TranslationServiceImpl implements TranslationService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JsonObject callbackParameters;

    private final List<String> translationLanguages;

    private final DatasetManager datasetManager;
    private final TripleStore tripleStore;
    private final IndexService indexService;

    private final HttpRequest<Buffer> translationServiceRequest;

    private final PiveauContext moduleContext;

    TranslationServiceImpl(
            Vertx vertx,
            WebClient client,
            JsonObject config,
            TripleStore tripleStore,
            Handler<AsyncResult<TranslationService>> readyHandler) {

        JsonObject translationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE);
        translationLanguages = translationConfig.getJsonArray("accepted_languages").getList();

        translationServiceRequest = client.postAbs(translationConfig.getString("translation_service_url"))
                .putHeader("Content-Type", "application/json")
                .expect(ResponsePredicate.SC_SUCCESS);

        callbackParameters = new JsonObject()
                .put("url", translationConfig.getString("callback_url"))
                .put("method", "POST")
                .put("headers", new JsonObject()
                        .put("Authorization", config.getString(Constants.ENV_PIVEAU_HUB_API_KEY)));

        datasetManager = tripleStore.getDatasetManager();
        this.tripleStore = tripleStore;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);

        moduleContext = new PiveauContext("hub", "Translation");

        readyHandler.handle(Future.succeededFuture(this));
    }

    /**
     * Extract title and description from Dataset and its Distributions and send them to the translation service.
     *<p>
     * If the Dataset is only an update of an old Dataset with no titles or descriptions changed, nothing will be send to the translation service.
     *
     * @param helper the new Dataset with new and not yet translated fields
     * @param oldHelper the optional old Dataset with already translated fields, should be `null`, if there is no old Dataset
     * @param asyncHandler the {@link Handler} which handles the result
     * @return this {@link TranslationService} for a fluent usage
     */
    @Override
    public TranslationService initializeTranslationProcess(DatasetHelper helper,DatasetHelper oldHelper, Handler<AsyncResult<DatasetHelper>> asyncHandler) {
        PiveauContext resourceContext = moduleContext.extend(helper.id());

        resourceContext.log().debug("Resource: {}, model:\n{}", helper.resource().getURI(), helper.stringify(Lang.TURTLE));

        addCatalogRecordDetailsBeforeTranslation(helper);

        final JsonObject requestBody = buildJsonFromResource(helper, oldHelper);
        //if requestBody is null, everything is already translated, this function can now return
        if(requestBody==null){
            asyncHandler.handle(Future.succeededFuture(helper));
            return this;
        }

        translationServiceRequest.sendJsonObject(requestBody, ar -> {
            if(ar.succeeded()) {
                resourceContext.log().info("Translation initialized.");
                resourceContext.log().debug("Translation request: {}", requestBody.encodePrettily());
                asyncHandler.handle(Future.succeededFuture(helper));
            } else {
                resourceContext.log().error("Translation initialization failed.", ar.cause());
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });

        return this;
    }

    @Override
    public TranslationService receiveTranslation(JsonObject translation, Handler<AsyncResult<JsonObject>> asyncHandler) {
        PiveauContext resourceContext = moduleContext.extend(translation.getString("id"));
        resourceContext.log().debug("Incoming translation: {}", translation.encodePrettily());
        DCATAPUriRef uriRef = DCATAPUriSchema.applyFor(translation.getString("id"));
        datasetManager.getGraph(uriRef.getDatasetGraphName(), ar -> {
            if(ar.succeeded()) {

                String originalLanguage = translation.getString("original_language");
                JsonObject translations = translation.getJsonObject("translation");

                Model model = ar.result();

                Resource resource = model.getResource(uriRef.getDatasetUriRef());
                addTranslationsToModel(resource, translations, originalLanguage);

                // Updating catalog record with translation information
                Resource record = model.getResource(uriRef.getRecordUriRef());
                addCatalogRecordDetailsAfterTranslation(record, originalLanguage);

                // Write model back to store and index
                sendTranslationToStore(uriRef, model);

                JsonObject payload = translation.getJsonObject("payload", new JsonObject());
                sendTranslationToIndex(resource, payload.getString("catalogueId"), payload.getString("defaultLanguage"));

                resourceContext.log().info("Translation stored");
                asyncHandler.handle(Future.succeededFuture(new JsonObject().put("status", "success")));
            } else {
                resourceContext.log().error("Translation received: " + ar.cause().getMessage(), ar.cause());
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });

        return this;
    }

    private void addTranslationsToModel(Resource resource, JsonObject translations, String originalLanguage) {
        removeOldTranslations(resource);
        for(String language : translations.fieldNames()) {
            String languageTag = buildLanguageTag(originalLanguage, language);
            JsonObject attributes = translations.getJsonObject(language);
            for(String attribute : attributes.fieldNames()) {
                if("title".equals(attribute)) {
                    resource.addProperty(DCTerms.title, attributes.getString(attribute), languageTag);
                } else if("description".equals(attribute)) {
                    resource.addProperty(DCTerms.description, attributes.getString(attribute), languageTag);
                } else {
                    String distributionId = attribute.substring(0, attribute.length() - 4);
                    String distributionUriRef = DCATAPUriSchema.applyFor(distributionId).getDistributionUriRef();
                    Resource distribution = resource.getModel().getResource(distributionUriRef);
                    if(attribute.endsWith("titl")) {
                        distribution.addProperty(DCTerms.title, attributes.getString(attribute), languageTag);
                    } else if(attribute.endsWith("desc")) {
                        distribution.addProperty(DCTerms.description, attributes.getString(attribute), languageTag);
                    }
                }
            }
        }
    }

    private void removeOldTranslations(Resource resource) {
        // Just remove all "-t0-mtec"
        removeTranslations(resource, DCTerms.title);
        removeTranslations(resource, DCTerms.description);

        resource.listProperties(DCAT.distribution)
                .filterKeep(statement -> statement.getObject().isURIResource())
                .mapWith(Statement::getResource)
                .forEachRemaining(distribution -> {
                    removeTranslations(distribution, DCTerms.title);
                    removeTranslations(distribution, DCTerms.description);
                });
    }

    private void addCatalogRecordDetailsBeforeTranslation(DatasetHelper helper) {
        Resource record = helper.recordResource();

        record.removeAll(EDP.translationReceived);
        record.removeAll(EDP.translationIssued);
        record.addProperty(EDP.translationIssued, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
        record.removeAll(EDP.translationStatus);
        record.addProperty(EDP.translationStatus, EDP.TransInProcess);
    }

    private void addCatalogRecordDetailsAfterTranslation(Resource record, String originalLanguage) {
        record.removeAll(EDP.translationReceived);
        record.addProperty(EDP.translationReceived, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
        record.removeAll(EDP.originalLanguage);
        record.addProperty(EDP.originalLanguage, ResourceFactory.createResource(codes.get(originalLanguage)));
        record.removeAll(EDP.translationStatus);
        record.addProperty(EDP.translationStatus, EDP.TransCompleted);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }

    private void sendTranslationToStore(DCATAPUriRef uriRef, Model model) {
        datasetManager.setGraph(uriRef.getDatasetGraphName(), model, ar -> {
            if(ar.succeeded()) {
                log.debug("Dataset updated with translation information in store successful.");
            } else {
                log.error("Put dataset", ar.cause());
            }
        });
    }

    private void sendTranslationToIndex(Resource resource, String catalogueId, String defaultLanguage) {
        if(catalogueId == null || defaultLanguage == null) {
            tripleStore.select("SELECT ?c ?l WHERE { ?c <http://www.w3.org/ns/dcat#dataset> <" + resource.getURI() + "> ; <http://purl.org/dc/terms/language> ?l }", ar -> {
                if(ar.succeeded()) {
                    ResultSet set = ar.result();
                    if(set.hasNext()) {
                        QuerySolution solution = set.next();
                        RDFNode c = solution.get("c");
                        RDFNode l = solution.get("l");
                        String language = Languages.INSTANCE.iso6391Code(Languages.INSTANCE.getConcept(l.asResource()));
                        String id = DCATAPUriSchema.parseUriRef(c.asResource().getURI()).getId();
                        indexService.addDatasetPut(Indexing.indexingDataset(resource, id, language), ir -> {
                            if(ir.succeeded()) {
                                log.debug("Successfully send to Index Service");
                            } else {
                                log.error("Dataset could not send to IndexService", ir.cause());
                            }
                        });
                    } else {
                        log.error("Selecting catalogueId and defaultLanguage. No result");
                    }
                } else {
                    log.error("Selecting catalogueId and defaultLanguage", ar.cause());
                }
            });
        } else {
            indexService.addDatasetPut(Indexing.indexingDataset(resource, catalogueId, defaultLanguage), ir -> {
                if(ir.succeeded()) {
                    log.debug("Successfully send to Index Service");
                } else {
                    log.error("Dataset could not send to IndexService", ir.cause());
                }
            });
        }
    }

    private JsonObject buildJsonFromResource(DatasetHelper helper, DatasetHelper oldHelper) {
        JsonObject requestBody = new JsonObject();

        List<String> availableLanguages = getAvailableLanguages(helper.resource(), helper.sourceLang());
        List<String> languages = new ArrayList<>(translationLanguages);
        languages.removeAll(availableLanguages);

        // Adding languages for translation request
        requestBody.put("languages", new JsonArray(languages));

        // Adding original language
        final String originalLanguage = availableLanguages.isEmpty() || availableLanguages.contains(helper.sourceLang()) ? helper.sourceLang() : availableLanguages.get(0);
        requestBody.put("original_language", originalLanguage);

        //get dict of new or untranslated fields. If we get back `null`, there are no new||untranslated fields and we can cancel this translation and return
        JsonObject dataDict = getDataDictIfNew(helper, oldHelper, originalLanguage);
        if(dataDict==null)return null;

        // Adding data dict to translate
        requestBody.put("data_dict", dataDict);

        // Adding callback parameters
        requestBody.put("callback", getCallbackParameters(helper));

        return requestBody;
    }


    private JsonObject getDataDictIfNew(DatasetHelper helper, DatasetHelper oldHelper, String originalLanguage) {

        //if there is no old data, we just return the whole dict
        if(oldHelper == null) {

            return getDataDict(helper.resource(), originalLanguage);
        }

        //get data dict for the old model, before it was updated
        JsonObject oldData = getDataDict(oldHelper.resource(), originalLanguage);

        //get data dict for the new, updated model
        JsonObject newData = getDataDict(helper.resource(), originalLanguage);

        //iterate through data dict of new model and remove
        newData.fieldNames()
                .removeIf(
                        key -> oldData.containsKey(key) &&
                                oldData.getString(key).equals(newData.getString(key))
                );

        if(newData.isEmpty()) {
            return null;
        } else {
            return newData;
        }
    }


    private JsonObject getCallbackParameters(DatasetHelper helper) {
        return callbackParameters.copy()
                .put("payload", new JsonObject()
                        .put("id", DCATAPUriSchema.parseUriRef(helper.uriRef()).getId())
                        .put("catalogueId", helper.catalogueId())
                        .put("defaultLanguage", helper.sourceLang()));
    }

}
