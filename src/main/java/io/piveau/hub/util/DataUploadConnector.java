package io.piveau.hub.util;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.dcatap.DCATAPUriRef;
import io.piveau.dcatap.DCATAPUriSchema;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;

import java.util.UUID;

public class DataUploadConnector {

    PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

    private WebClient client;
    private String url;
    private String serviceUrl;
    private String apiKey;

    public static DataUploadConnector create(WebClient client, JsonObject config) {
        return new DataUploadConnector(client, config);
    }

    private DataUploadConnector(WebClient client, JsonObject config) {
        this.client = client;
        this.url = config.getString("url");
        this.serviceUrl = config.getString("service_url");
        this.apiKey = config.getString("api_key");
    }

    public String getDataURL(String distributionID) {
        return url + "/v1/data/" + distributionID;
    }

    public JsonArray getResponse(DatasetHelper helper) {
        JsonArray response = new JsonArray();
        helper.resource().listProperties(DCAT.distribution)
                .filterKeep(statement -> statement.getObject().isResource())
                .mapWith(Statement::getResource)
                .forEachRemaining(distribution -> {
                    DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(distribution.getURI());
                    if (schema != null && distribution.hasProperty(DCAT.accessURL)) {
                        String token = UUID.randomUUID().toString();
                        JsonObject dist = new JsonObject()
                                .put("id", schema.getId())
                                .put("access_url", distribution.getPropertyResourceValue(DCAT.accessURL).getURI())
                                .put("upload_url", distribution.getPropertyResourceValue(DCAT.accessURL).getURI() + "?token=" + token)
                                .put("upload_token", token);
                        if (distribution.hasProperty(DCTerms.title)) {
                            dist.put("title", distribution.getProperty(DCTerms.title).getLiteral().getLexicalForm());
                        }
                        if (distribution.hasProperty(DCTerms.identifier)) {
                            dist.put("identifier", distribution.getProperty(DCTerms.identifier).getLiteral().getLexicalForm());
                        }
                        if (distribution.hasProperty(DCTerms.format)) {
                            RDFNode formatProp = distribution.getProperty(DCTerms.format).getObject();
                            if (formatProp.isLiteral()) {
                                dist.put("format", distribution.getProperty(DCTerms.format).getLiteral().getLexicalForm());
                            }
                            if (formatProp.isResource()) {
                                dist.put("format", distribution.getProperty(DCTerms.format).getResource().getURI());
                            }
                        }
                        if (dist.containsKey("access_url") && dist.getValue("access_url").toString().contains(this.url)) {
                            response.add(dist);
                        }
                    }
                });

        prepareDataService(response);
        return response;
    }

    private void prepareDataService(JsonArray distributions) {
        JsonArray payload = new JsonArray();
        distributions.iterator().forEachRemaining(o -> {
            JsonObject file = new JsonObject();
            JsonObject dist = (JsonObject) o;
            file.put("id", dist.getValue("id"));
            file.put("token", dist.getValue("upload_token"));
            payload.add(file);
        });

        LOGGER.info(payload.toString());

        HttpRequest<Buffer> request = client.putAbs(this.serviceUrl + "/v1/data/")
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json");

        request.sendJson(payload, ar -> {
            if (ar.succeeded()) {
                if (ar.result().statusCode() == 200) {
                    LOGGER.info("Successful requested Data Upload Service");
                } else {
                    LOGGER.error("Error when requested Data Upload Service " + ar.result().bodyAsString());
                }
            } else {
                LOGGER.error("Error when requested Data Upload Service " + ar.cause());
            }
        });
    }
}
