package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.kvs.RowData;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.webserver.Route;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.post;

class Worker extends cis5550.generic.Worker {

    private static final Logger logger = Logger.getLogger(Worker.class);
    private static File myJAR;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        registerRoutes();
    }

    private static void registerRoutes() {
        flatMap();
        mapToPair();
        flatMapToPair();
        fold();
        foldByKey();
        intersection();
        sample();
        fromTable();
        join();
        filter();
        mapPartitions();
        cogroup();
        union();
        distinct();
    }

    private static void flatMap() {
        postWrapper("/rdd/flatMap", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String pairRDD = request.queryParams("pair");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            if (Objects.equals(pairRDD, "true")) {
                FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable)
                        Serializer.byteArrayToObject(lambdaBytes, myJAR);
                for (Iterator<Row> rows : iterators) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String key = row.key();
                        for (String column : row.columns()) {
                            String value = row.get(column);
                            FlamePair pair = new FlamePair(key, value);
                            Iterable<String> results = lambda.op(pair);
                            if (results != null) {
                                for (String result : results) {
                                    if (result != null) {
                                        String newRowKey = Hasher.hash(generateRandomID());
                                        Coordinator.kvs.put(outputTable, newRowKey, "value", result);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable)
                        Serializer.byteArrayToObject(lambdaBytes, myJAR);
                for (Iterator<Row> rows : iterators) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String value = row.get("value");
                        Iterable<String> results = lambda.op(value);
                        if (results != null) {
                            for (String result : results) {
                                if (result != null) {
                                    String newRowKey = Hasher.hash(generateRandomID());
                                    Coordinator.kvs.put(outputTable, newRowKey, "value", result);
                                }
                            }
                        }
                    }
                }
            }

            return "OK";
        });
    }

    private static void mapToPair() {
        postWrapper("/rdd/mapToPair", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    FlamePair pair = lambda.op(value);
                    if (pair != null && pair._2() != null) {
                        Coordinator.kvs.put(outputTable, pair._1(), row.key(), pair._2());
                    }
                }
            }

            return "OK";
        });
    }

    private static void flatMapToPair() {
        postWrapper("/rdd/flatMapToPair", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String pairRDD = request.queryParams("pair");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            if (Objects.equals(pairRDD, "true")) {
                FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable)
                        Serializer.byteArrayToObject(lambdaBytes, myJAR);
                for (Iterator<Row> rows : iterators) {
                    List<RowData> batch = new ArrayList<>();
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String key = row.key();
                        for (String column : row.columns()) {
                            String value = row.get(column);
                            if (value != null) {
                                FlamePair pair = new FlamePair(key, value);
                                Iterable<FlamePair> pairs = lambda.op(pair);
                                if (pairs != null) {
                                    for (FlamePair newPair : pairs) {
                                        if (newPair != null && newPair._2() != null) {
                                            RowData data = new RowData(newPair._1(), generateRandomID(), newPair._2());
                                            batch.add(data);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!batch.isEmpty()) {
                        Coordinator.kvs.putRows(outputTable, batch);
                    }
                }
            } else {
                FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable)
                        Serializer.byteArrayToObject(lambdaBytes, myJAR);
                for (Iterator<Row> rows : iterators) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String value = row.get("value");
                        if (value != null) {
                            Iterable<FlamePair> pairs = lambda.op(value);
                            if (pairs != null) {
                                for (FlamePair pair : pairs) {
                                    if (pair != null && pair._2() != null) {
                                        Coordinator.kvs.put(outputTable, pair._1(), generateRandomID(), pair._2());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return "OK";
        });
    }

    private static void fold() {
        postWrapper("/rdd/fold", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String zeroElement = request.queryParams("zeroElement");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            String accumulator = zeroElement == null ? "" : zeroElement;

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        accumulator = lambda.op(accumulator, value);
                    }
                }
            }

            String newRowKey = Hasher.hash(generateRandomID());
            Coordinator.kvs.put(outputTable, newRowKey, "value", accumulator);

            return "OK";
        });
    }

    private static void foldByKey() {
        postWrapper("/rdd/foldByKey", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String zeroElement = request.queryParams("zeroElement");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                List<RowData> batch = new ArrayList<>();
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String key = row.key();
                    String accumulator = zeroElement;
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        accumulator = lambda.op(accumulator, value);
                    }
                    batch.add(new RowData(key, "acc", accumulator));
                }
                if (!batch.isEmpty()) {
                    Coordinator.kvs.putRows(outputTable, batch);
                }
            }

            return "OK";
        });
    }

    private static void intersection() {
        postWrapper("/rdd/intersection", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String otherTable = request.queryParams("otherTable");

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Create a set to store values from the other table
            Set<String> otherValues = new HashSet<>();

            // Scan the other table and collect all its values
            Iterator<Row> otherRows = Coordinator.kvs.scan(otherTable);
            while (otherRows.hasNext()) {
                Row row = otherRows.next();
                String value = row.get("value");
                otherValues.add(value);
            }

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            Set<String> uniqueValues = new HashSet<>();

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    // Check if the value exists in the other table
                    if (value != null && otherValues.contains(value)) {
                        uniqueValues.add(value);
                    }
                }
            }

            for (String value : uniqueValues) {
                String newRowKey = Hasher.hash(generateRandomID());
                Coordinator.kvs.put(outputTable, newRowKey, "value", value);
            }

            return "OK";
        });
    }

    private static void sample() {
        postWrapper("/rdd/sample", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String fractionStr = request.queryParams("fraction");

            double fraction = Double.parseDouble(fractionStr);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get iterators for the input table
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            Random random = new Random();

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    if (value != null && random.nextDouble() <= fraction) {
                        String newRowKey = Hasher.hash(generateRandomID());
                        Coordinator.kvs.put(outputTable, newRowKey, "value", value);
                    }
                }
            }

            return "OK";
        });
    }

    private static void fromTable() {
        postWrapper("/rdd/fromTable", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String result = lambda.op(row);
                    if (result != null) {
                        String newRowKey = Hasher.hash(generateRandomID());
                        Coordinator.kvs.put(outputTable, newRowKey, "value", result);
                    }
                }
            }

            return "OK";
        });
    }

    // private static void join() {
    //     postWrapper("/rdd/join", (request, response) -> {
    //         // Get query parameters
    //         String inputTable = request.queryParams("inputTable");
    //         String outputTable = request.queryParams("outputTable");
    //         String kvsMaster = request.queryParams("kvsMaster");
    //         String fromKey = request.queryParams("fromKey");
    //         String toKeyExclusive = request.queryParams("toKeyExclusive");
    //         String otherTable = request.queryParams("otherTable");

    //         // Get the KVS client
    //         Coordinator.kvs = new KVSClient(kvsMaster);

    //         // Get the iterators
    //         List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

    //         for (Iterator<Row> rows : iterators) {
    //             List<RowData> batch = new ArrayList<>();
    //             while (rows.hasNext()) {
    //                 Row row = rows.next();
    //                 String key = row.key();

    //                 // Check if the key exists in the other table
    //                 Row otherRow = Coordinator.kvs.getRow(otherTable, key);
    //                 if (otherRow != null) {
    //                     Set<String> firstColumns = row.columns();
    //                     Set<String> secondColumns = otherRow.columns();

    //                     // Parallelize the nested loop
    //                     List<RowData> partialBatch = firstColumns.parallelStream()
    //                             .flatMap(firstColumn -> {
    //                                 String firstValue = row.get(firstColumn);
    //                                 return secondColumns.parallelStream()
    //                                         .map(secondColumn -> {
    //                                             String secondValue = otherRow.get(secondColumn);
    //                                             String combinedValue = firstValue + "," + secondValue;

    //                                             String firstColumnName = Hasher.hash(firstColumn);
    //                                             String secondColumnName = Hasher.hash(secondColumn);
    //                                             String combinedColumnName = firstColumnName + "|" + secondColumnName;

    //                                             return new RowData(key, combinedColumnName, combinedValue);
    //                                         });
    //                             })
    //                             .toList();

    //                     // Add to batch
    //                     batch.addAll(partialBatch);
    //                 }
    //             }
    //             if (!batch.isEmpty()) {
    //                 logger.info("Batch size: " + batch.size());
    //                 Coordinator.kvs.putRows(outputTable, batch);
    //             }
    //         }

    //         return "OK";
    //     });
    // }

    private static void join() {
        postWrapper("/rdd/join", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String otherTable = request.queryParams("otherTable");

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String key = row.key();

                    // Check if the key exists in the other table
                    if (Coordinator.kvs.existsRow(otherTable, key)) {
                        Row otherRow = Coordinator.kvs.getRow(otherTable, key);

                        Set<String> firstColumns = row.columns();
                        Set<String> secondColumns = otherRow.columns();

                        for (String firstColumn : firstColumns) {
                            String firstValue = row.get(firstColumn);
                            for (String secondColumn : secondColumns) {
                                String secondValue = otherRow.get(secondColumn);
                                String combinedValue = firstValue + "," + secondValue;

                                String firstColumnName = Hasher.hash(firstColumn);
                                String secondColumnName = Hasher.hash(secondColumn);
                                String combinedColumnName = firstColumnName + "|" + secondColumnName;

                                Coordinator.kvs.put(outputTable, key, combinedColumnName, combinedValue);
                            }
                        }
                    }
                }
            }

            return "OK";
        });
    }

    private static void filter() {
        postWrapper("/rdd/filter", ((request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    if (value != null && lambda.op(value)) {
                        String newRowKey = Hasher.hash(generateRandomID());
                        Coordinator.kvs.put(outputTable, newRowKey, "value", value);
                    }
                }
            }

            return "OK";
        }));
    }

    private static void mapPartitions() {
        postWrapper("/rdd/mapPartitions", ((request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            // Deserialize the lambda
            byte[] lambdaBytes = request.bodyAsBytes();
            FlameRDD.IteratorToIterator lambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(lambdaBytes, myJAR);

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);

            for (Iterator<Row> rows : iterators) {
                // not use a list to collect all values, but use an iterator to iterate
                Iterator<String> values = new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return rows.hasNext();
                    }

                    @Override
                    public String next() {
                        Row row = rows.next();
                        String value = row.get("value");
                        return value != null ? value : "";
                    }
                };

                Iterator<String> newValues = lambda.op(values);
                while (newValues.hasNext()) {
                    String value = newValues.next();
                    if (value != null) {
                        String newRowKey = Hasher.hash(generateRandomID());
                        Coordinator.kvs.put(outputTable, newRowKey, "value", value);
                    }
                }
            }

            return "OK";
        }));
    }

    private static void cogroup() {
        postWrapper("/rdd/cogroup", (request, response) -> {
            // Get query parameters
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String otherTable = request.queryParams("otherTable");

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> inputIterators = getIterators(fromKey, toKeyExclusive, inputTable, Coordinator.kvs);
            List<Iterator<Row>> otherIterators = getIterators(fromKey, toKeyExclusive, otherTable, Coordinator.kvs);

            // Collect all keys from both tables
            Set<String> allKeys = new HashSet<>();
            for (Iterator<Row> rows : inputIterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    allKeys.add(row.key());
                }
            }

            for (Iterator<Row> rows : otherIterators) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    allKeys.add(row.key());
                }
            }

            // Process each key
            for (String key : allKeys) {
                StringBuilder inputValuesBuilder = new StringBuilder();
                StringBuilder otherValuesBuilder = new StringBuilder();

                // Process values from inputTable
                if (Coordinator.kvs.existsRow(inputTable, key)) {
                    Row row = Coordinator.kvs.getRow(inputTable, key);
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        inputValuesBuilder.append(value).append(",");
                    }
                }

                // Process values from otherTable
                if (Coordinator.kvs.existsRow(otherTable, key)) {
                    Row row = Coordinator.kvs.getRow(otherTable, key);
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        otherValuesBuilder.append(value).append(",");
                    }
                }

                // Construct combined value
                String combinedValue = "[" + inputValuesBuilder.toString() + "],[" + otherValuesBuilder.toString() + "]";

                // Store in outputTable
                Coordinator.kvs.put(outputTable, key, "value", combinedValue);
            }

            return "OK";
        });
    }

    private static void union() {
        postWrapper("/rdd/union", (request, response) -> {
            // Get query parameters
            String inputTable1 = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String otherTable = request.queryParams("otherTable");

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Copy entries from inputTable1
            Iterator<Row> iterator1 = Coordinator.kvs.scan(inputTable1);
            while (iterator1.hasNext()) {
                Row row = iterator1.next();
                String value = row.get("value");
                if (value != null) {
                    String newRowKey = Hasher.hash(generateRandomID());
                    Coordinator.kvs.put(outputTable, newRowKey, "value", value);
                }
            }

            // Copy entries from otherTable
            Iterator<Row> iterator2 = Coordinator.kvs.scan(otherTable);
            while (iterator2.hasNext()) {
                Row row = iterator2.next();
                String value = row.get("value");
                if (value != null) {
                    String newRowKey = Hasher.hash(generateRandomID());
                    Coordinator.kvs.put(outputTable, newRowKey, "value", value);
                }
            }

            return "OK";
        });
    }

    private static void distinct() {
        postWrapper("/rdd/distinct", (request, response) -> {
            // Get query parameters
            String inputTable1 = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            // Get the KVS client
            Coordinator.kvs = new KVSClient(kvsMaster);

            // Get the iterators
            List<Iterator<Row>> iterators = getIterators(fromKey, toKeyExclusive, inputTable1, Coordinator.kvs);

            // Create a set to track seen values
            Set<String> seen = new HashSet<>();

            // Process each iterator
            for (Iterator<Row> iterator : iterators) {
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    String value = row.get("value");
                    if (value != null && seen.add(value)) { // Add to 'seen' if not already present
                        try {
                            // Use value as the key to ensure uniqueness
                            Coordinator.kvs.put(outputTable, value, "value", value);
                        } catch (IOException e) {
                            logger.error("Error writing to KVS in distinct: " + e.getMessage(), e);
                        }
                    }
                }
            }

            return "OK";
        });
    }

    // ----------------------- HELPER FUNCTIONS --------------------------- //

    private static void postWrapper(String s, Route route) {
        post(s, (request, response) -> {
            try {
                return route.handle(request, response);
            } catch (Exception e) {
                logger.error("Error in route: " + s, e);
                System.out.println("Error in route: " + s);

                response.status(500, "Internal Server Error");
                return "Error in route: " + s + "\n" + e.getMessage();
            }
        });
    }

    private static List<Iterator<Row>> getIterators(String fromKey,
                                                    String toKeyExclusive,
                                                    String inputTable,
                                                    KVSClient kvs) throws IOException {
        // Scan the input table and apply the lambda
        List<Iterator<Row>> iterators = new ArrayList<>();

        // Adjust fromKey and toKeyExclusive if they are null
        if (fromKey != null && fromKey.equals("null")) {
            fromKey = null;
        }
        if (toKeyExclusive != null && toKeyExclusive.equals("null")) {
            toKeyExclusive = null;
        }

        if (fromKey == null && toKeyExclusive == null) {
            iterators.add(kvs.scan(inputTable));
        } else if (fromKey != null && toKeyExclusive != null && fromKey.compareTo(toKeyExclusive) > 0) {
            // If fromKey > toKeyExclusive, we need to do wrap-around scan
            iterators.add(kvs.scan(inputTable, fromKey, null));
            iterators.add(kvs.scan(inputTable, null, toKeyExclusive));
        } else {
            iterators.add(kvs.scan(inputTable, fromKey, toKeyExclusive));
        }

        return iterators;
    }

    // copied from kvs worker
    private static String generateRandomID() {
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append((char) (Math.random() * 26 + 97));
        }
        return id.toString();
    }
}
