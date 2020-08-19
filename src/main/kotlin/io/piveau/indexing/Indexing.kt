@file:JvmName("Indexing")

package io.piveau.indexing

import io.piveau.dcatap.*
import io.piveau.json.*
import io.piveau.rdf.*
import io.piveau.vocabularies.vocabulary.EDP
import io.piveau.utils.normalizeDateTime
import io.piveau.vocabularies.CorporateBodies
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.apache.jena.rdf.model.*
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.*
import java.time.Instant

fun indexingCatalogue(catalogue: Resource): JsonObject = JsonObject().apply {
    if (catalogue.isURIResource) {
        putIfNotNull("id", DCATAPUriSchema.parseUriRef(catalogue.uri).id)
        putIfNotNull("idName", DCATAPUriSchema.parseUriRef(catalogue.uri).id)
        put("issued", Instant.now())
        put("modified", Instant.now())

        catalogue.listProperties().forEachRemaining { (_, predicate, obj) ->
            when (predicate) {
                // mandatory
                DCTerms.description -> indexingSimpleMultiLang(obj).apply {
                    withJsonObject("description").putIfNotNull(first, second)
                }
                DCTerms.publisher -> putIfNotEmpty("publisher", indexingPublisher(obj))
                DCTerms.title -> indexingSimpleMultiLang(obj).apply {
                    withJsonObject("title").putIfNotNull(first, second)
                }

                // recommended
//                FOAF.homepage -> {}
                DCTerms.language -> withJsonArray("languages").addIfNotNull(obj.visitWith(LanguageVisitor))
//                DCTerms.license -> {}
                DCTerms.issued -> putIfNotNull("issued", indexingDateTime(obj))
//                DCAT.themeTaxonomy -> {}
                DCTerms.modified -> putIfNotNull("modified", indexingDateTime(obj))

                // optional
//                DCTerms.hasPart -> {}
//                DCTerms.isPartOf -> {}
//                DCTerms.rights -> {}


                DCTerms.spatial -> putIfNotEmpty("country", obj.visitWith(SpatialVisitor) as JsonObject)
            }
        }
    }
}

fun indexingDataset(dataset: Resource, catalogueId: String? = null, defaultLang: String): JsonObject = JsonObject().apply {
    if (dataset.isURIResource) {
        putIfNotNull("id", DCATAPUriSchema.parseUriRef(dataset.uri).id)
        putIfNotNull("idName", DCATAPUriSchema.parseUriRef(dataset.uri).id)
        putIfNotEmpty("catalog", JsonObject().putIfNotNull("id", catalogueId))

        dataset.listProperties().forEachRemaining { (_, predicate, obj) ->
            when (predicate) {
                // mandatory
                DCTerms.description -> withJsonObject("description").putPair(indexingMultiLang(obj, defaultLang))
                DCTerms.title -> withJsonObject("title").putPair(indexingMultiLang(obj, defaultLang))

                // recommended
                DCAT.contactPoint -> withJsonArray("contact_points").addIfNotEmpty(indexingContactPoint(obj))
                DCAT.distribution -> withJsonArray("distributions").addIfNotEmpty(indexingDistribution(obj, defaultLang))
                DCAT.keyword -> withJsonArray("keywords").addIfNotEmpty(indexingKeyword(obj))
                DCTerms.publisher -> putIfNotEmpty("publisher", indexingPublisher(obj))
                DCAT.theme -> withJsonArray("categories").addIfNotEmpty(obj.visitWith(ThemeVisitor) as JsonObject)

                // optional
                DCTerms.accessRights -> putIfNotNull("access_right", obj.visitWith(StandardVisitor))
                DCTerms.conformsTo -> withJsonArray("conforms_to").addIfNotEmpty(obj.visitWith(ConformsToVisitor) as JsonObject)
                FOAF.page -> withJsonArray("documentations").addIfNotNull(obj.visitWith(StandardVisitor))
//                DCTerms.accrualPeriodicity -> {}
//                DCTerms.hasVersion -> {}
//                DCTerms.identifier -> {}
//                DCTerms.isVersionOf -> {}
                DCAT.landingPage -> withJsonArray("landing_page").addIfNotNull(obj.visitWith(StandardVisitor))
                DCTerms.language -> withJsonArray("languages").addIfNotNull(obj.visitWith(LanguageVisitor))
//                ADMS.identifier -> {}
                DCTerms.provenance -> withJsonArray("provenances").addIfNotEmpty(obj.visitWith(ProvenanceVisitor) as JsonObject)
//                DCTerms.relation -> {}
                DCTerms.issued -> putIfNotNull("release_date", indexingDateTime(obj))
//                ADMS.sample -> {}
//                DCTerms.source -> {}
                DCTerms.spatial -> putIfNotEmpty("spatial", obj.visitWith(GeoSpatialVisitor) as JsonObject)
//                DCTerms.temporal -> {}
//                DCTerms.type -> {}
                DCTerms.modified -> putIfNotNull("modification_date", indexingDateTime(obj))
//                OWL2.versionInfo -> {}
//                ADMS.versionNotes -> {}
            }
        }
        legacyTranslationMeta(this, dataset)
    }
}

fun indexingDistribution(node: RDFNode, defaultLang: String): JsonObject = when (node) {
    is Resource -> JsonObject().apply {
        putIfNotNull("id", DCATAPUriSchema.parseUriRef(node.uri).id)
        node.listProperties().forEachRemaining { (_, predicate, obj) ->
            when (predicate) {
                // mandatory
                DCAT.accessURL -> putIfNotNull("access_url", obj.visitWith(StandardVisitor))

                // recommended
                DCTerms.description -> withJsonObject("description").putPair(indexingMultiLang(obj, defaultLang))
                DCTerms.format -> putIfNotEmpty("format", obj.visitWith(FormatVisitor) as JsonObject)
                DCTerms.license -> putIfNotEmpty("licence", obj.visitWith(LicenseVisitor) as JsonObject)

                // optional
//                DCAT.byteSize -> {}
//                SPDX.checksum -> {}
//                FOAF.page -> {}
                DCAT.downloadURL -> withJsonArray("download_urls").addIfNotNull(obj.visitWith(StandardVisitor))
//                DCTerms.language -> {}
//                DCTerms.conformsTo -> {}
                DCAT.mediaType -> putIfNotNull("media_type", obj.visitWith(MediaTypeVisitor))
//                DCTerms.issued -> {}
//                DCTerms.rights -> {}
//                ADMS.status -> {}
                DCTerms.title -> withJsonObject("title").putPair(indexingMultiLang(obj, defaultLang))
//                DCTerms.modified -> {}
            }
        }
    }
    else -> JsonObject() // it is not a resource
}

fun indexingSimpleMultiLang(node: RDFNode): Pair<String, String?> = when (node) {
    is Literal -> Pair(node.language, node.string)
    else -> Pair("", null)
}

fun indexingMultiLang(node: RDFNode, defaultLang: String): Pair<String, JsonObject?> = when (node) {
    is Literal -> {
        val (machineTranslated, language, originalLanguage) = parseLangTag(node.language, defaultLang)
        val lang = JsonObject().put("value", node.string).put("machineTranslated", machineTranslated)
            .putIfNotNull("originalLanguage", originalLanguage)
        Pair(language, lang)
    }
    else -> Pair("", null)
}

fun indexingKeyword(node: RDFNode): JsonObject = when (node) {
    is Literal -> JsonObject().put("id", node.lexicalForm.asNormalized()).put("title", node.string)
    else -> JsonObject()
}

fun indexingPublisher(node: RDFNode): JsonObject = when (node) {
    is Resource -> JsonObject().apply {
        CorporateBodies.getConcept(node)?.let {
            put("type",
                it.resource.selectFrom(listOf(FOAF.Agent, FOAF.Person, FOAF.Organization))?.localName
                    ?: FOAF.Organization.localName
            )
            putIfNotNull("name", it.label("en"))
            putIfNotNull("homepage", it.resource.getPropertyResourceValue(FOAF.homepage)?.uri)
            putIfNotNull("email", it.resource.getPropertyResourceValue(FOAF.mbox)?.uri)
            put("resource", it.resource.uri)
        } ?: put(
            "type",
            node.selectFrom(listOf(FOAF.Agent, FOAF.Person, FOAF.Organization))?.localName ?: FOAF.Agent.localName
        ).also {
            node.listProperties().forEachRemaining { (_, predicate, obj) ->
                when (predicate) {
                    FOAF.name -> put("name", obj.visitWith(StandardVisitor))
                    FOAF.homepage -> put("homepage", obj.visitWith(StandardVisitor))
                    FOAF.mbox -> put("email", obj.visitWith(StandardVisitor))
                }
            }
        }
    }
    else -> JsonObject() // is not a resource
}

fun indexingContactPoint(node: RDFNode): JsonObject = when (node) {
    is Resource -> JsonObject().apply {
        put(
            "type",
            node.selectFrom(listOf(VCARD4.Kind, VCARD4.Individual, VCARD4.Organization))?.localName
                ?: VCARD4.Kind.localName
        )

        node.listProperties().forEachRemaining { (_, predicate, obj) ->
            when (predicate) {
                VCARD4.fn, VCARD4.hasName -> put("name", obj.visitWith(VCARDVisitor))
                VCARD4.hasEmail -> put("email", obj.visitWith(VCARDVisitor))
            }
        }
    }
    else -> JsonObject() // is not a resource
}

fun indexingDateTime(node: RDFNode): String? = when (node) {
    is Literal -> normalizeDateTime(node.string)
    else -> null
}

fun legacyTranslationMeta(index: JsonObject, dataset: Resource) {
    val recordInfo = JsonObject().apply {
        DCATAPUriSchema.parseUriRef(dataset.uri).recordUriRef.let {
            val record = dataset.model.getResource(it)
            putIfNotNull("received", record?.getProperty(EDP.translationReceived)?.string)
            putIfNotNull("issued", record?.getProperty(EDP.translationIssued)?.string)
            putIfNotNull("status", record?.getProperty(EDP.translationStatus)?.resource?.let { status ->
                when (status) {
                    EDP.TransCompleted -> "completed"
                    EDP.TransInProcess -> "processing"
                    else -> "unknown"
                }
            } ?: "unknown")
        }
    }

    val translationMeta = JsonObject()
    val details = translationMeta.withJsonObject("details")
    val title = index.getJsonObject("title")
    title?.forEach { (key, value) ->
        details.withJsonObject(key)
            .putIfNotNull("machine_translated", (value as JsonObject).getBoolean("machineTranslated"))
            .putIfNotNull("original_language", value.getString("originalLanguage"))
            .putIfNotNull("received", recordInfo.getString("received"))
            .putIfNotNull("issued", recordInfo.getString("issued"))
        title.put(key, (value).getString("value"))
    }
    translationMeta.put("full_available_languages", details.fieldNames().toList())
    translationMeta.putIfNotNull("status", recordInfo.getString("status"))
    index.put("translation_meta", translationMeta)

    val description = index.getJsonObject("description")
    description?.forEach { (key, value) ->
        description.put(key, (value as JsonObject).getString("value"))
    }

    index.getJsonArray("distributions", JsonArray())?.forEach {
        val distributionTitle = (it as JsonObject).getJsonObject("title")
        distributionTitle?.forEach { (key, value) ->
            distributionTitle.put(key, (value as JsonObject).getString("value"))
        }
        val distributionDescription = it.getJsonObject("description")
        distributionDescription?.forEach { (key, value) ->
            distributionDescription.put(key, (value as JsonObject).getString("value"))
        }
    }
}
