package com.google.cloud.storage.contrib.nio.testing;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.spi.v1.RpcBatch;
import com.google.cloud.storage.spi.v1.StorageRpc;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class FakeRpcBatch implements RpcBatch {

    protected StorageRpc storageRpc;
    protected Collection<scala.Tuple3<StorageObject, RpcBatch.Callback<Void>, Map<StorageRpc.Option, ?>>> deletions;

    public FakeRpcBatch(StorageRpc rpc) {
        storageRpc = rpc;
        deletions = new ArrayList<>();
    }

    @Override
    public void addDelete(StorageObject storageObject, RpcBatch.Callback<Void> callback, Map<StorageRpc.Option, ?> options) {
        deletions.add(new Tuple3<>(storageObject, callback, options));
    }

    @Override
    public void addPatch(StorageObject storageObject, RpcBatch.Callback<StorageObject> callback, Map<StorageRpc.Option, ?> options) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void addGet(StorageObject storageObject, RpcBatch.Callback<StorageObject> callback, Map<StorageRpc.Option, ?> options) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void submit() {
        deletions.forEach(tuple -> {
            storageRpc.delete(tuple._1(), tuple._3());
            tuple._2().onSuccess(null);
        });
    }
}