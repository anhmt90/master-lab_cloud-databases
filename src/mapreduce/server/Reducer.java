package mapreduce.server;

import ecs.KeyHashRange;
import server.api.BatchDataTransferProcessor;

import java.util.Set;

public abstract class Reducer<KT, VT> extends MapReduce<KT, VT>{
    public abstract void reduce();

    public abstract Set<KT> getKeySet();

    public Reducer(String dbPath, KeyHashRange appliedRange, String prefix) {
        super(prefix);
        BatchDataTransferProcessor batchProcessor = new BatchDataTransferProcessor(dbPath, prefix);
        String[] indexFiles = batchProcessor.indexData(appliedRange);
        collectFiles(indexFiles);
    }
}
