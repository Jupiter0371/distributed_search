package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FlameContextImpl implements FlameContext, Serializable {

    private final String jarName;
    private final List<String> outputs;

    private static int rddCounter = 0;
    private final List<String> flameWorkers;
    private final Partitioner partitioner;
    private Vector<Partitioner.Partition> partitions;

    private static final Logger logger = Logger.getLogger(FlameContextImpl.class);

    public FlameContextImpl(String jarName, List<String> flameWorkers) throws IOException {
        this.jarName = jarName;
        this.outputs = new ArrayList<>();
        this.flameWorkers = flameWorkers;
        this.partitioner = new Partitioner();

        // Assign partitions
        assignPartitions();
    }

    @Override
    public KVSClient getKVS() {
        return Coordinator.kvs;
    }

    @Override
    public void output(String s) {
        outputs.add(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        // Generate a unique table name
        String tableName = "rdd-" + "parallelize-" + System.currentTimeMillis() + "-" + rddCounter++;

        // Upload the strings to the table
        for (String s : list) {
            String rowKey = Hasher.hash(generateRandomID());
            Coordinator.kvs.put(tableName, rowKey, "value", s);
        }

        logger.info("Flame RDD parallelized for table " + tableName);

        return new FlameRDDImpl(tableName, this);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        // Serialize the lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String operation = "rdd/fromTable";
        String outputTableName = invokeOperation(operation, serializedLambda, tableName, null);
        return new FlameRDDImpl(outputTableName, this);
    }

    public String getOutputs() {
        StringBuilder sb = new StringBuilder();
        for (String s : outputs) {
            sb.append(s);
        }
        return sb.toString();
    }

    public String invokeOperation(String operation, byte[] lambda, String inputTableName, Map<String, String> params) throws Exception {
        String outputTableName;
        if ("rdd/mapPartitions".equals(operation)) {
            if (inputTableName.equals("rdd-frontier-A")) {
                // Table is A; store in B
                outputTableName = "rdd-frontier-B";
                Coordinator.kvs.delete("rdd-frontier-B");
            } else {
                // Table is B or initial; store in A
                outputTableName = "rdd-frontier-A";
                Coordinator.kvs.delete("rdd-frontier-A");
            }
        } else {
            // Generate a unique table name
            outputTableName = "rdd-" + operation.substring(4) + "-" + System.currentTimeMillis() + "-" + rddCounter++;
        }

        logger.info("Started: Operation " + operation + " started on table " + inputTableName + " with output table " + outputTableName);

        // Send requests to Flame workers
        sendRequest(operation, lambda, partitions, inputTableName, outputTableName, params);
        logger.info("Completed: Operation " + operation + " completed on table " + inputTableName + " with output table " + outputTableName);

        return outputTableName;
    }

    private void sendRequest(String operation,
                                    byte[] lambda,
                                    Vector<Partitioner.Partition> partitions,
                                    String inputTableName,
                                    String outputTableName,
                                    Map<String, String> params) throws IOException, InterruptedException {
        List<Thread> threads = new ArrayList<>();
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

        for (Partitioner.Partition partition : partitions) {
            Runnable task = () -> {
                try {
                    String flameWorkerAddress = partition.assignedFlameWorker;
                    StringBuilder urlBuilder = new StringBuilder();
                    urlBuilder.append("http://").append(flameWorkerAddress).append("/").append(operation).append("?");
                    urlBuilder.append("inputTable=").append(URLEncoder.encode(inputTableName, StandardCharsets.UTF_8));
                    urlBuilder.append("&outputTable=").append(URLEncoder.encode(outputTableName, StandardCharsets.UTF_8));
                    urlBuilder.append("&kvsMaster=").append(URLEncoder.encode(Coordinator.kvs.getCoordinator(), StandardCharsets.UTF_8));
                    if (partition.fromKey != null) {
                        urlBuilder.append("&fromKey=").append(URLEncoder.encode(partition.fromKey, StandardCharsets.UTF_8));
                    }
                    if (partition.toKeyExclusive != null) {
                        urlBuilder.append("&toKeyExclusive=").append(URLEncoder.encode(partition.toKeyExclusive, StandardCharsets.UTF_8));
                    }
                    if (params != null) {
                        for (Map.Entry<String, String> entry : params.entrySet()) {
                            urlBuilder.append("&").append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                            urlBuilder.append("=").append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
                        }
                    }

                    HTTP.Response response = HTTP.doRequest("POST", urlBuilder.toString(), lambda);

                    if (response.statusCode() != 200) {
                        synchronized (exceptions) {
                            exceptions.add(new Exception("HTTP request failed with status code " + response.statusCode()));
                        }
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                }
            };
            Thread thread = new Thread(task);
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Check if any exceptions occurred
        if (!exceptions.isEmpty()) {
            throw new IOException("One or more worker requests failed", exceptions.get(0));
        }
    }

    private void assignPartitions() throws IOException {
        // Get the number of KVS workers
        int numWorkers = Coordinator.kvs.numWorkers();
        if (numWorkers == 0) {
            throw new IOException("No KVS workers available");
        }

        // Add KVS workers with correct key ranges
        for (int i = 0; i < numWorkers - 1; i++) {
            String kvsWorkerID = Coordinator.kvs.getWorkerID(i);
            String kvsWorkerAddress = Coordinator.kvs.getWorkerAddress(i);
            String nextWorkerID = Coordinator.kvs.getWorkerID(i + 1);
            partitioner.addKVSWorker(kvsWorkerAddress, kvsWorkerID, nextWorkerID);
        }

        // Last worker
        String lastWorkerID = Coordinator.kvs.getWorkerID(numWorkers - 1);
        String lastWorkerAddress = Coordinator.kvs.getWorkerAddress(numWorkers - 1);
        String firstWorkerID = Coordinator.kvs.getWorkerID(0);
        partitioner.addKVSWorker(lastWorkerAddress, lastWorkerID, null);
        partitioner.addKVSWorker(lastWorkerAddress, null, firstWorkerID);

        // Call addFlameWorker for each Flame worker
        for (String worker : flameWorkers) {
            partitioner.addFlameWorker(worker);
        }

        // Call the Partitioner’s assignPartitions() method
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
        if (partitions == null || partitions.isEmpty()) {
            logger.error("No partitions available");
            return;
        }

        logger.info("Flame workers partitions assigned");
        this.partitions = partitions;
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
