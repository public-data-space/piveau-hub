package io.piveau.hub.services.metrics;

import io.piveau.hub.util.Constants;
import io.piveau.pipe.PiveauCluster;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.TripleStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class MetricsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        ConfigHelper configHelper = ConfigHelper.forConfig(config());
        JsonObject clusterConfig = configHelper.forceJsonObject(Constants.ENV_PIVEAU_CLUSTER_CONFIG);

        TripleStore tripleStore = new TripleStore(vertx, ConfigHelper.forConfig(vertx.getOrCreateContext().config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG), null);

        PiveauCluster.create(vertx, clusterConfig)
                .onSuccess(cluster ->
                        MetricsService.create(tripleStore, config(), cluster.pipeLauncher(vertx), ready -> {
                            if (ready.succeeded()) {
                                new ServiceBinder(vertx).setAddress(MetricsService.SERVICE_ADDRESS).register(MetricsService.class, ready.result());
                                startPromise.complete();
                            } else {
                                startPromise.fail(ready.cause());
                            }
                        }))
                .onFailure(startPromise::fail);
    }

}
