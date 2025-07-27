package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.util.*;

public class FlameRDDImpl implements FlameRDD {

    private String tableName;
    private final FlameContextImpl context;

    private static final Logger logger = Logger.getLogger(FlameRDDImpl.class);

    public FlameRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    @Override
    public int count() throws Exception {
        return Coordinator.kvs.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Coordinator.kvs.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String operation = "rdd/distinct";
        String outputTableName = context.invokeOperation(operation, null, tableName, null);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public void destroy() throws Exception {
        Coordinator.kvs.delete(tableName);
        tableName = null;
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        if (num <= 0) {
            throw new IllegalArgumentException("Number of elements to take must be greater than 0");
        }

        // return the first num rows
        Vector<String> result = new Vector<>();
        Iterator<Row> iterator = Coordinator.kvs.scan(tableName);
        int count = 0;

        while (iterator.hasNext() && count < num) {
            Row row = iterator.next();
            String value = row.get("value");
            result.add(value);
            count++;
        }

        return result;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Prepare the zero element
        Map<String, String> params = new HashMap<>();
        params.put("zeroElement", zeroElement);

        String operation = "rdd/fold";
        String partialResultsTableName = context.invokeOperation(operation, serializedLambda, tableName, params);
        String finalAccumulator = zeroElement;

        // Scan the partial results table and fold the values
        Iterator<Row> iterator = Coordinator.kvs.scan(partialResultsTableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String value = row.get("value");
            finalAccumulator = lambda.op(finalAccumulator, value);
        }

        // Delete the partial results table
        Coordinator.kvs.delete(partialResultsTableName);

        return finalAccumulator;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        Iterator<Row> iterator = Coordinator.kvs.scan(tableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String value = row.get("value");
            result.add(value);
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/flatMap";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, null);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/flatMapToPair";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, null);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/mapToPair";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, null);
        return new FlamePairRDDImpl(outputTableName, context);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        if (!(r instanceof FlameRDDImpl otherRDD)) {
            throw new IllegalArgumentException("Can only intersect with another FlameRDD");
        }

        // Prepare the other table name
        Map<String, String> params = new HashMap<>();
        params.put("otherTable", otherRDD.tableName);

        String operation = "rdd/intersection";
        String outputTableName = context.invokeOperation(operation, null, tableName, params);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        if (f <= 0 || f >= 1) {
            throw new IllegalArgumentException("Sample fraction must be between 0 and 1");
        }

        // Prepare the sample fraction
        Map<String, String> params = new HashMap<>();
        params.put("fraction", String.valueOf(f));

        String operation = "rdd/sample";
        String outputTableName = context.invokeOperation(operation, null, tableName, params);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/filter";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, null);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/mapPartitions";
        String outputTableName = context.invokeOperation(operation, serializedLambda, tableName, null);
        return new FlameRDDImpl(outputTableName, context);
    }

    @Override
    public FlameRDD union(FlameRDD r) throws Exception {
        if (!(r instanceof FlameRDDImpl otherRDD)) {
            throw new IllegalArgumentException("Can only union with another FlameRDD");
        }

        // Prepare the output table name
        String outputTableName = "union-" + System.currentTimeMillis();

        // Combine the tables on the workers
        Map<String, String> params = new HashMap<>();
        params.put("otherTable", otherRDD.tableName);

        String operation = "rdd/union";
        String resultTable = context.invokeOperation(operation, null, this.tableName, params);
        return new FlameRDDImpl(resultTable, context);
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}
