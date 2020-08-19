package io.piveau.hub.services.distributions;


import io.piveau.dcatap.TripleStore;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.json.ConfigHelper;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class DistributionsServiceVerticle  extends AbstractVerticle {

        @Override
        public void start(Promise<Void> startPromise) {
            WebClient client = WebClient.create(vertx);

            JsonObject conf = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_TRIPLESTORE_CONFIG);

            CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                    .retryPolicy(count -> count * 1000L);

            TSConnector connector = TSConnector.create(client, breaker, conf);
            IndexService indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
            TranslationService translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);

            TripleStore tripleStore = new TripleStore(vertx, conf);

            DistributionsService.create(connector, tripleStore, indexService, translationService, ready -> {
                if (ready.succeeded()) {
                    new ServiceBinder(vertx).setAddress(DistributionsService.SERVICE_ADDRESS).register(DistributionsService.class, ready.result());
                    startPromise.complete();
                } else {
                    startPromise.fail(ready.cause());
                }
            });
        }

    }
