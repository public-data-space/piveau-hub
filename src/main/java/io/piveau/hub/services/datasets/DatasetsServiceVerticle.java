package io.piveau.hub.services.datasets;

import io.piveau.hub.util.Constants;
import io.piveau.hub.util.DataUploadConnector;
import io.piveau.hub.util.TSConnector;
import io.piveau.pipe.PiveauCluster;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.TripleStore;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class DatasetsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        WebClient client = WebClient.create(vertx);

        ConfigHelper configHelper = ConfigHelper.forConfig(config());
        JsonObject conf = configHelper.forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG);
        JsonObject dataUploadConf = configHelper.forceJsonObject(Constants.ENV_PIVEAU_DATA_UPLOAD);
        JsonObject clusterConfig = configHelper.forceJsonObject(Constants.ENV_PIVEAU_CLUSTER_CONFIG);

        PiveauCluster.create(vertx, clusterConfig).onComplete(init -> {
            if (init.succeeded()) {
                CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                        .retryPolicy(count -> count * 1000L);

                TSConnector connector = TSConnector.create(client, breaker, conf);
                TripleStore tripleStore = new TripleStore(vertx, conf, null);

                DataUploadConnector dataUploadConnector = DataUploadConnector.create(client, dataUploadConf);

                DatasetsService.create(tripleStore, connector, dataUploadConnector, config(), init.result().pipeLauncher(vertx), vertx, ready -> {
                    if (ready.succeeded()) {
                        new ServiceBinder(vertx).setAddress(DatasetsService.SERVICE_ADDRESS).register(DatasetsService.class, ready.result());
                        startPromise.complete();
                    } else {
                        startPromise.fail(ready.cause());
                    }
                });
            } else {
                startPromise.fail(init.cause());
            }
        });
    }

}
