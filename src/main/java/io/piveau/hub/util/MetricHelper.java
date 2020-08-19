package io.piveau.hub.util;

import io.piveau.utils.JenaUtils;
import io.piveau.dcatap.DCATAPUriRef;
import io.piveau.dcatap.DCATAPUriSchema;
import io.piveau.vocabularies.vocabulary.SPDX;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

public class MetricHelper {
    private String id;
    private String hash;
    private DCATAPUriRef uriSchema;
    private Model model;

    private MetricHelper(Model model, Handler<AsyncResult<MetricHelper>> handler) {
        try {
            this.model = model;
            hash = JenaUtils.canonicalHash(model);
            extractId();
            handler.handle(Future.succeededFuture(this));
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private MetricHelper(String id, Model model, String hash, Handler<AsyncResult<MetricHelper>> handler) {
        this(model, ar -> {
            if (ar.succeeded()) {
                MetricHelper helper = ar.result();
                helper.id = id;

                if (helper.uriSchema == null) {
                    helper.uriSchema = DCATAPUriSchema.applyFor(id);
                }

                if (hash != null && !hash.isEmpty()) {
                    helper.hash = hash;
                }
                handler.handle(Future.succeededFuture(helper));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }

    public static void create(Model model, Handler<AsyncResult<MetricHelper>> handler) {
        new MetricHelper(model, handler);
    }

    public static void create(String id, Model model, String hash, Handler<AsyncResult<MetricHelper>> handler) {
        new MetricHelper(id, model, hash, handler);
    }

    public String id() {
        return id;
    }

    public String hash() {
        return hash;
    }

    public String uriRef() {
        return uriSchema.getDatasetUriRef();
    }

    public String metricRef() {
        return uriSchema.getMetricsUriRef();
    }

    public String metricGraphName() {
        return uriSchema.getMetricsGraphName();
    }

    public Resource resource() {
        return model.getResource(uriRef());
    }

    public Model model() {
        return model;
    }

    public String stringify(Lang lang) {
        return JenaUtils.write(model, lang);
    }

    public void update(Model oldModel, String recordUriRef) {
        uriSchema = DCATAPUriSchema.applyFor(DCATAPUriSchema.parseUriRef(recordUriRef).getId());

        Model recordModel = JenaUtils.extractResource(oldModel.getResource(recordUriRef));
        Resource record = recordModel.getResource(recordUriRef);
        updateRecord(record, hash);

        renameReferences(null);

        model.add(recordModel);
    }

    private void updateRecord(Resource record, String hash) {
        Resource checksum = record.getPropertyResourceValue(SPDX.checksum);
        checksum.removeAll(SPDX.checksumValue);
        checksum.addProperty(SPDX.checksumValue, hash);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }

    private void renameReferences(Map<String, Resource> identDist) {
        //rename datasets
        model.listSubjectsWithProperty(RDF.type, DCAT.Dataset).forEachRemaining(ds -> ResourceUtils.renameResource(ds, uriRef()));

        //rename distributions
        model.listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(resource -> {

            //set the id to lookup in the identDist map
            String id;
            if (resource.hasProperty(DCTerms.identifier)) {
                id = resource.getProperty(DCTerms.identifier).getLiteral().toString();
            } else if (resource.isURIResource()) {
                id = resource.getURI();
            } else if (resource.hasProperty(DCTerms.title)) {
                id = resource.getProperty(DCTerms.title).getLiteral().toString();
            } else {
                id = resource.getProperty(DCAT.accessURL).getResource().getURI();
            }

            //if the Distribution in the new model has an {@link DCTerms#identifier identifier} that was already present in the old model,
            //      give the new Distribution the same id/uriRef as the old distribution
            //else give it a new uriRef
            //
            if (identDist.containsKey(id)) {
                Resource renamed = ResourceUtils.renameResource(resource, identDist.get(id).getURI());
                renamed.addProperty(DCTerms.identifier, id);
            } else {

                String newID = UUID.randomUUID().toString();

                Resource renamed = ResourceUtils.renameResource(resource, DCATAPUriSchema.applyFor(newID).getDistributionUriRef());
                // if there is no {@link DCTerms#identifier identifier}, add a the new one we can use on the next update
                if (!renamed.hasProperty(DCTerms.identifier)) {
                    renamed.addProperty(DCTerms.identifier, id);
                }
            }
        });
    }

    private void extractId() {
        ResIterator it = model.listSubjectsWithProperty(RDF.type, DCAT.CatalogRecord);
        if (it.hasNext()) {
            Resource record = it.next();
            if (record.getProperty(DCTerms.identifier) != null) {
                id = record.getProperty(DCTerms.identifier).getObject().asLiteral().getString();
            }
        }

        it = model.listSubjectsWithProperty(RDF.type, DCAT.Dataset);
        if (it.hasNext()) {
            Resource dataset = it.next();
            if (dataset.getURI() != null) {
                DCATAPUriRef tmp = DCATAPUriSchema.parseUriRef(dataset.getURI());
                if (tmp != null) {
                    uriSchema = DCATAPUriSchema.applyFor(tmp.getId());
                }
            }
        }
    }

}
