package io.piveau.hub.services;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class DatasetHelperMessageCodec implements MessageCodec<DatasetHelper, DatasetHelper> {

    @Override
    public void encodeToWire(Buffer buffer, DatasetHelper entries) {
    }

    @Override
    public DatasetHelper decodeFromWire(int i, Buffer buffer) {
        return null;
    }

    @Override
    public DatasetHelper transform(DatasetHelper helper) {
        return helper;
    }

    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

}
