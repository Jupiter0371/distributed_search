package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.util.*;

public class FlamePairRDDImpl implements FlamePairRDD {

    private String tableName;
    private final FlameContextImpl context;

    private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);

    public FlamePairRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();
        Iterator<Row> iterator = Coordinator.kvs.scan(tableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            Set<String> columns = row.columns();
            String key = row.key();
            for (String column : columns) {
                String value = row.get(column);
                result.add(new FlamePair(key, value));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Prepare zero element
        Map<String, String> params = new HashMap<>();
        params.put("zeroElement", zeroElement);

        String operation = "rdd/foldByKey";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, params);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        context.getKVS().rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Indicate this is a pair RDD
        Map<String, String> params = new HashMap<>();
        params.put("pair", "true");

        String operation = "rdd/flatMap";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, params);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public void destroy() throws Exception {
        Coordinator.kvs.delete(tableName);
        tableName = null;
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Indicate this is a pair RDD
        Map<String, String> params = new HashMap<>();
        params.put("pair", "true");

        String operation = "rdd/flatMapToPair";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, params);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        if (!(other instanceof FlamePairRDDImpl otherRDD)) {
            throw new IllegalArgumentException("Can only join with another FlamePairRDD");
        }

        // Prepare the other table name
        Map<String, String> params = new HashMap<>();
        params.put("otherTable", otherRDD.tableName);

        String operation = "rdd/join";
        String outputTableName = context.invokeOperation(operation, null, tableName, params);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        if (!(other instanceof FlamePairRDDImpl otherRDD)) {
            throw new IllegalArgumentException("Can only cogroup with another FlamePairRDD");
        }

        // Prepare the other table name
        Map<String, String> params = new HashMap<>();
        params.put("otherTable", otherRDD.tableName);

        String operation = "rdd/cogroup";
        String outputTableName = context.invokeOperation(operation, null, tableName, params);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}
