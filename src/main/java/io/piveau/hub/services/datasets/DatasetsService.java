package io.piveau.hub.services.datasets;

import io.piveau.hub.util.DataUploadConnector;
import io.piveau.hub.util.TSConnector;
import io.piveau.pipe.PipeLauncher;
import io.piveau.dcatap.TripleStore;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface DatasetsService {
    String SERVICE_ADDRESS = "io.piveau.hub.datasets.queue";

    static DatasetsService create(TripleStore tripleStore, TSConnector connector, DataUploadConnector dataUploadConnector, JsonObject config, PipeLauncher launcher, Vertx vertx, Handler<AsyncResult<DatasetsService>> readyHandler) {
        return new DatasetsServiceImpl(tripleStore, connector, dataUploadConnector, config, launcher, vertx, readyHandler);
    }

    static DatasetsService createProxy(Vertx vertx, String address) {
        return new DatasetsServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    DatasetsService listDatasets(String consumes, String catalogueId, Integer limit, Integer offset, Boolean sourceIds, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService getDataset(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService getDatasetByNormalizedId(String datasetSuffix, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService putDataset(String datasetId, String dataset, String contentType, String catalogueId, String hash, Boolean createAccessURLs, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService postDataset(String dataset, String contentType, String catalogueId, Boolean createAccessURLs, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService deleteDataset(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService getRecord(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService indexDataset(String datasetId, String catalogueId, String defaultLang, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService getDataUploadInformation(String datasetId, String catalogueId, String resultDataset, Handler<AsyncResult<JsonObject>> handler);
}
