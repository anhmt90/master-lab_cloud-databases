package mapreduce.server;

import ecs.KeyHashRange;
import server.api.BatchDataTransferProcessor;
import util.StringUtils;

public abstract class Mapper<KT, VT> extends MapReduce<KT, VT>{
    public abstract void map();

    public Mapper(String dbPath, KeyHashRange appliedRange) {
        super(StringUtils.EMPTY_STRING);
        String[] indexFiles = new BatchDataTransferProcessor(dbPath).indexData(appliedRange);
        collectFiles(indexFiles);
    }
}
