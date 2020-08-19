package io.piveau.hub.services.catalogues;

import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.json.ConfigHelper;
import io.piveau.dcatap.TripleStore;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class CataloguesServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        PiveauLoggerFactory.getLogger(getClass()).info("Starting CataloguesService Verticle");

        WebClient client = WebClient.create(vertx);

        ConfigHelper configHelper = ConfigHelper.forConfig(config());
        JsonObject conf = configHelper.forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG);

        CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                .retryPolicy(count -> count * 1000L);

        TSConnector connector = TSConnector.create(client, breaker, conf);

        TripleStore tripleStore = new TripleStore(vertx, conf, null);

        CataloguesService.create(tripleStore, connector, vertx, ready -> {
            if (ready.succeeded()) {
                new ServiceBinder(vertx).setAddress(CataloguesService.SERVICE_ADDRESS).register(CataloguesService.class, ready.result());
                startPromise.complete();
            } else {
                startPromise.fail(ready.cause());
            }
        });
    }
}
