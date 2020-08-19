package io.piveau.hub.util;

import io.piveau.utils.JenaUtils;
import io.piveau.dcatap.DCATAPUriSchema;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class CatalogueHelper {
    private String id;
    private String contentType;
    private String content;

    public Model getModel() {
        return model;
    }

    private Model model;

    public String uriRef() {
        return DCATAPUriSchema.applyFor(id).getCatalogueUriRef();
    }

    public CatalogueHelper(String id, String contentType, String content) {
        this.id = id;
        this.contentType = contentType;
        this.content = content;
    }
    public String getId(){return id;}

    public void
    model(Map<String, List<Resource>> saved, Handler<AsyncResult<Void>> handler) {
        if (model == null) {
            try {
                model = JenaUtils.read(content.getBytes(), contentType);
                renameReferences();

                Resource catalog = model.getResource(uriRef());
                saved.get("dataset").forEach(resource -> {
                    catalog.addProperty(DCAT.dataset, resource);
                });
                saved.get("record").forEach(resource -> {
                    catalog.addProperty(DCAT.record, resource);
                });
                modify(catalog);

            } catch (Exception e) {
                handler.handle(Future.failedFuture(e));
                return;
            }
        }
        handler.handle(Future.succeededFuture());
    }

    public void model(Handler<AsyncResult<Void>> handler) {
        if (model == null) {
            try {
                model = JenaUtils.read(content.getBytes(), contentType);

                renameReferences();
                modify(model.getResource(uriRef()));
                model.getResource(uriRef()).addProperty(DCTerms.issued, ZonedDateTime
                        .now(ZoneOffset.UTC)
                        .truncatedTo(ChronoUnit.SECONDS)
                        .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
            } catch (Exception e) {
                handler.handle(Future.failedFuture(e));
                return;
            }
        }
        handler.handle(Future.succeededFuture());
    }

    private void renameReferences() {
        model.listSubjectsWithProperty(RDF.type, DCAT.Catalog).forEachRemaining(ds -> {
            ResourceUtils.renameResource(ds, uriRef());
        });
    }

    private void modify(Resource catalog) {
        catalog.removeAll(DCTerms.modified);
        catalog.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }


}
