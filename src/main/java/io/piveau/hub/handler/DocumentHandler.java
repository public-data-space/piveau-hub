package io.piveau.hub.handler;

import io.piveau.hub.util.Constants;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

abstract class DocumentHandler {

    void listDocuments(RoutingContext context, String address){

        context.vertx().eventBus().request(address, null, ar -> {
            if (ar.succeeded()) {
                JsonObject j = new JsonObject(ar.result().body().toString());

                context.response().setStatusCode(200);
                context.response().putHeader("Content-Type", "application/json");
                context.response().end(new JsonObject().put("success", true).mergeIn(j).encode());
            } else {
                fail(context,ar);
            }
        });
    }

    void putDocument(RoutingContext context, String address){

        JsonObject message = new JsonObject().put("model", context.getBodyAsString());

        String id = context.pathParam("id");
        if (id!=null && !id.isEmpty()) {

            message.put("id",id);
        }

        List<String> catalog = context.queryParam("catalog");
        if( !catalog.isEmpty() && catalog.size()==1){
            message.put("catalog",catalog.get(0));
        }




        message.put("mimeType", context.parsedHeaders().contentType().value());

        context.vertx().eventBus().request(address, message, ar -> {
            if (ar.succeeded()) {
                JsonObject j = new JsonObject(ar.result().body().toString());
                context.response().putHeader("Content-Type", "application/json");
                context.response().setStatusCode(j.getInteger("statusCode", 200));
                context.response().putHeader("Location", j.getString("location", ""));
                context.response().end(new JsonObject().put("success", true).put("model", j.getString("model")).encode());
            } else {
                fail(context, ar);
            }
        });
    }


    void postDocument(RoutingContext context, String address){

        JsonObject message = new JsonObject().put("model", context.getBodyAsString());
        message.put("mimeType", context.parsedHeaders().contentType().value());

        context.vertx().eventBus().request(address, message, ar -> {
            if (ar.succeeded()) {
                JsonObject j = new JsonObject(ar.result().body().toString());

                context.response().setStatusCode(201);
                context.response().putHeader("Content-Type", "application/json");
                context.response().putHeader("Location", j.getString("location"));
                context.response().end(new JsonObject().put("success", true).put("model", j.getString("model")).encode());
            } else {
                fail(context, ar);
            }
        });
    }

    void getDocument(RoutingContext context, String address){
        JsonObject jo = new JsonObject();
        jo.put("id",context.pathParam("id"));
        if(!context.parsedHeaders().accept().isEmpty())  jo.put("accept", context.parsedHeaders().accept().get(0).rawValue());
        context.vertx().eventBus().request(address, jo, ar -> {
            if (ar.succeeded()) {

                String model = ar.result().body().toString();
                context.response().setStatusCode(200);
                context.response().putHeader("Content-Type", jo.getString("accept", "application/n-triples"));
                context.response().end(model);
            } else {
                fail(context, ar);
            }
        });
    }

    void deleteDocument(RoutingContext context, String address){
        context.vertx().eventBus().request(address, context.pathParam("id"), ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200);
                context.response().end(new JsonObject().put("success", true).encode());
            } else {
                fail(context, ar);
            }
        });
    }

    private void fail(RoutingContext context, AsyncResult<Message<Object>> ar) {
        context.response().putHeader("Content-Type", "application/json");
        int failcode = ((ReplyException)ar.cause()).failureCode();
        int code = failcode>0?failcode:500;
        context.response().setStatusCode(code);
        context.response().end(new JsonObject().put("success", false).put("cause", ar.cause().getMessage()).encode());
    }

}
