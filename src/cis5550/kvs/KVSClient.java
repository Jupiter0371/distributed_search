package cis5550.kvs;

import cis5550.tools.HTTP;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class KVSClient implements KVS {

    String coordinator;

    static class WorkerEntry implements Comparable<WorkerEntry> {
        String address;
        String id;

        WorkerEntry(String addressArg, String idArg) {
            address = addressArg;
            id = idArg;
        }

        public int compareTo(WorkerEntry e) {
            return id.compareTo(e.id);
        }
    }

    Vector<WorkerEntry> workers;
    boolean haveWorkers;

    private static final Logger logger = Logger.getLogger(KVSClient.class);
    private static final int MAX_BATCH_SIZE = 20000;

    public int numWorkers() throws IOException {
        if (!haveWorkers)
            downloadWorkers();
        return workers.size();
    }

    public static String getVersion() {
        return "v1.4 Aug 5 2023";
    }

    public String getCoordinator() {
        return coordinator;
    }

    public String getWorkerAddress(int idx) throws IOException {
        if (!haveWorkers)
            downloadWorkers();
        return workers.elementAt(idx).address;
    }

    public String getWorkerID(int idx) throws IOException {
        if (!haveWorkers)
            downloadWorkers();
        return workers.elementAt(idx).id;
    }

    class KVSIterator implements Iterator<Row> {
        InputStream in;
        boolean atEnd;
        Row nextRow;
        int currentRangeIndex;
        String endRowExclusive;
        String startRow;
        String tableName;
        Vector<String> ranges;

        KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
            in = null;
            currentRangeIndex = 0;
            atEnd = false;
            endRowExclusive = endRowExclusiveArg;
            tableName = tableNameArg;
            startRow = startRowArg;
            ranges = new Vector<String>();
            if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
                String url = getURL(tableNameArg, numWorkers() - 1, startRowArg, ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0)) < 0)) ? endRowExclusiveArg : getWorkerID(0));
                ranges.add(url);
            }
            for (int i = 0; i < numWorkers(); i++) {
                if ((startRowArg == null) || (i == numWorkers() - 1) || (startRowArg.compareTo(getWorkerID(i + 1)) < 0)) {
                    if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
                        boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i)) > 0);
                        boolean useActualEndRow = (endRowExclusiveArg != null) && ((i == (numWorkers() - 1)) || (endRowExclusiveArg.compareTo(getWorkerID(i + 1)) < 0));
                        String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i), useActualEndRow ? endRowExclusiveArg : ((i < numWorkers() - 1) ? getWorkerID(i + 1) : null));
                        ranges.add(url);
                    }
                }
            }

            openConnectionAndFill();
        }

        protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
            String params = "";
            if (startRowArg != null)
                params = "startRow=" + startRowArg;
            if (endRowExclusiveArg != null)
                params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
            return "http://" + getWorkerAddress(workerIndexArg) + "/data/" + tableNameArg + (params.equals("") ? "" : "?" + params);
        }

        void openConnectionAndFill() {
            try {
                if (in != null) {
                    in.close();
                    in = null;
                }

                if (atEnd)
                    return;

                while (true) {
                    if (currentRangeIndex >= ranges.size()) {
                        atEnd = true;
                        return;
                    }

                    try {
                        URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
                        HttpURLConnection con = (HttpURLConnection) url.openConnection();
                        con.setRequestMethod("GET");
                        con.connect();
                        in = con.getInputStream();
                        Row r = fill();
                        if (r != null) {
                            nextRow = r;
                            break;
                        }
                    } catch (FileNotFoundException fnfe) {
                    } catch (URISyntaxException use) {
                    }

                    currentRangeIndex++;
                }
            } catch (IOException ioe) {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Exception e) {
                    }
                    in = null;
                }
                atEnd = true;
            }
        }

        synchronized Row fill() {
            try {
                Row r = Row.readFrom(in);
                return r;
            } catch (Exception e) {
                return null;
            }
        }

        public synchronized Row next() {
            if (atEnd)
                return null;
            Row r = nextRow;
            nextRow = fill();
            while ((nextRow == null) && !atEnd) {
                currentRangeIndex++;
                openConnectionAndFill();
            }

            return r;
        }

        public synchronized boolean hasNext() {
            return !atEnd;
        }
    }

    synchronized void downloadWorkers() throws IOException {
        String result = new String(HTTP.doRequest("GET", "http://" + coordinator + "/workers", null).body());
        String[] pieces = result.split("\n");
        int numWorkers = Integer.parseInt(pieces[0]);
        if (numWorkers < 1)
            throw new IOException("No active KVS workers");
        if (pieces.length != (numWorkers + 1))
            throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
        workers.clear();
        for (int i = 0; i < numWorkers; i++) {
            String[] pcs = pieces[1 + i].split(",");
            workers.add(new WorkerEntry(pcs[1], pcs[0]));
        }
        Collections.sort(workers);

        haveWorkers = true;
    }

    int workerIndexForKey(String key) {
        int chosenWorker = workers.size() - 1;
        if (key != null) {
            for (int i = 0; i < workers.size() - 1; i++) {
                if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i + 1).id) < 0))
                    chosenWorker = i;
            }
        }

        return chosenWorker;
    }

    public KVSClient(String coordinatorArg) {
        coordinator = coordinatorArg;
        workers = new Vector<WorkerEntry>();
        haveWorkers = false;
    }

    public boolean rename(String oldTableName, String newTableName) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        boolean result = true;
        for (WorkerEntry w : workers) {
            try {
                byte[] response = HTTP.doRequest("PUT",
                        "http://" + w.address +
                                "/rename/" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8) +
                                "/", newTableName.getBytes()).body();
                String res = new String(response);
                result &= res.equals("OK");
            } catch (Exception e) {
                result = false;
            }
        }

        return result;
    }

    public void delete(String oldTableName) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        for (WorkerEntry w : workers) {
            try {
                byte[] response = HTTP.doRequest("PUT",
                        "http://" + w.address +
                                "/delete/" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8) +
                                "/", null).body();
                String result = new String(response);
            } catch (Exception e) {
                throw new IOException("Failed to delete table " + oldTableName + " from worker " + w.address);
            }
        }
    }

    public void put(String tableName, String row, String column, byte[] value) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        String target = null;
        try {
            target = "http://" + workers.elementAt(workerIndexForKey(row)).address +
                    "/data/" + URLEncoder.encode(tableName, StandardCharsets.UTF_8) +
                    "/" + URLEncoder.encode(row, StandardCharsets.UTF_8) +
                    "/" + URLEncoder.encode(column, StandardCharsets.UTF_8);

            HTTP.Response response = HTTP.doRequest("PUT", target, value);
            int statusCode = response.statusCode();
            String result = new String(response.body());

            if (statusCode != 200) {
                throw new IOException("PUT request to " + target + " failed with status code " + statusCode + ": " + result);
            }

            if (!"OK".equals(result)) {
                throw new IOException("Unexpected response body from " + target + ": " + result);
            }
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("UTF-8 encoding not supported?!?", uee);
        } catch (IOException ioe) {
            throw new IOException("Failed to send PUT request to " + target, ioe);
        }
    }

    public void put(String tableName, String row, String column, String value) throws IOException {
        put(tableName, row, column, value.getBytes());
    }

    public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
        if (!haveWorkers)
            downloadWorkers();

        String target = null;
        try {
            target = "http://" + workers.elementAt(workerIndexForKey(row.key())).address +
                    "/data/" + URLEncoder.encode(tableName, StandardCharsets.UTF_8);
            HTTP.Response response = HTTP.doRequest("PUT", target, row.toByteArray());
            int statusCode = response.statusCode();
            String result = new String(response.body());

            if (statusCode != 200) {
                throw new IOException("PUT request to " + target + " failed with status code " + statusCode + ": " + result);
            }

            if (!"OK".equals(result)) {
                throw new IOException("Unexpected response body from " + target + ": " + result);
            }
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("UTF-8 encoding not supported?!?", uee);
        } catch (IOException ioe) {
            throw new IOException("Failed to send PUT request to " + target, ioe);
        }
    }

    public void putRows(String tableName, List<RowData> batch) throws IOException {
        if (!haveWorkers)
            downloadWorkers();
        
        if (batch == null || batch.isEmpty()) {
            throw new IOException("Batch PUT request failed: No rows provided in batch.");
        }

        // Map to hold rows grouped by worker index
        Map<Integer, List<RowData>> workerToRowsMap = new HashMap<>();

        // Group rows by their responsible worker
        for (RowData row : batch) {
            int workerIndex = workerIndexForKey(row.key);
            workerToRowsMap.computeIfAbsent(workerIndex, k -> new ArrayList<>()).add(row);
        }

        for (int workerIndex : workerToRowsMap.keySet()) {
            List<RowData> rowsForWorker = workerToRowsMap.get(workerIndex);
            logger.info("Worker " + workerIndex + " has " + rowsForWorker.size() + " rows to process.");

            // Partition the rows into smaller sub-batches
            List<List<RowData>> subBatches = partitionList(rowsForWorker);

            for (List<RowData> subBatch : subBatches) {
                // Serialize the sub-batch of rows into a single payload
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    for (RowData row : subBatch) {
                        baos.write(row.toByteArray());
                        baos.write('\n'); // Delimit rows with newline
                    }
                } catch (IOException e) {
                    logger.error("Failed to serialize sub-batch of rows: " + e);
                    continue; // Skip sending this sub-batch
                }
                byte[] batchPayload = baos.toByteArray();

                String target = null;
                try {
                    // Construct the target URL for the worker's batch endpoint
                    target = "http://" + workers.elementAt(workerIndex).address +
                            "/data/" + URLEncoder.encode(tableName, StandardCharsets.UTF_8) + "/batch";

                    HTTP.Response response = HTTP.doRequest("PUT", target, batchPayload);
                    int statusCode = response.statusCode();
                    String result = new String(response.body());

                    if (statusCode != 200) {
                        throw new IOException("Batch PUT request to " + target + " failed with status code " + statusCode + ": " + result);
                    }

                    if (!"OK".equals(result)) {
                        throw new IOException("Unexpected response body from " + target + ": " + result);
                    }
                }catch (UnsupportedEncodingException uee) {
                    throw new RuntimeException("UTF-8 encoding not supported?!?", uee);
                } catch (IOException ioe) {
                    throw new IOException("Failed to send batch PUT request to " + target, ioe);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Thread interrupted while waiting between worker batches.");
                }
            }
        }
    }

    private static <T> List<List<T>> partitionList(List<T> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += KVSClient.MAX_BATCH_SIZE) {
            partitions.add(list.subList(i, Math.min(i + KVSClient.MAX_BATCH_SIZE, list.size())));
        }
        return partitions;
    }

    public Row getRow(String tableName, String row) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        HTTP.Response resp = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + URLEncoder.encode(row, "UTF-8"), null);
        if (resp.statusCode() == 404)
            return null;

        byte[] result = resp.body();
        try {
            return Row.readFrom(new ByteArrayInputStream(result));
        } catch (Exception e) {
            throw new RuntimeException("Decoding error while reading Row from getRow() URL");
        }
    }

    public byte[] get(String tableName, String row, String column) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        HTTP.Response res = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + URLEncoder.encode(row, "UTF-8") + "/" + URLEncoder.encode(column, "UTF-8"), null);
        return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
    }

    public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
        if (!haveWorkers)
            downloadWorkers();

        HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + URLEncoder.encode(row, "UTF-8"), null);
        return r.statusCode() == 200;
    }

    public int count(String tableName) throws IOException {
        if (!haveWorkers)
            downloadWorkers();

        int total = 0;
        for (WorkerEntry w : workers) {
            HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + tableName, null);
            if ((r != null) && (r.statusCode() == 200)) {
                String result = new String(r.body());
                total += Integer.valueOf(result).intValue();
            }
        }
        return total;
    }

    public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
        return scan(tableName, null, null);
    }

    public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
        if (!haveWorkers)
            downloadWorkers();

        return new KVSIterator(tableName, startRow, endRowExclusive);
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 2) {
            System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
            System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
            System.err.println("Syntax: client <coordinator> scan <tableName>");
            System.err.println("Syntax: client <coordinator> delete <tableName>");
            System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
            System.exit(1);
        }

        KVSClient client = new KVSClient(args[0]);
        if (args[1].equals("put")) {
            if (args.length != 6) {
                System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
                System.exit(1);
            }
            client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
        } else if (args[1].equals("get")) {
            if (args.length != 5) {
                System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
                System.exit(1);
            }
            byte[] val = client.get(args[2], args[3], args[4]);
            if (val == null)
                System.err.println("No value found");
            else
                System.out.write(val);
        } else if (args[1].equals("scan")) {
            if (args.length != 3) {
                System.err.println("Syntax: client <coordinator> scan <tableName>");
                System.exit(1);
            }

            Iterator<Row> iter = client.scan(args[2], null, null);
            int count = 0;
            while (iter.hasNext()) {
                System.out.println(iter.next());
                count++;
            }
            System.err.println(count + " row(s) scanned");
        } else if (args[1].equals("delete")) {
            if (args.length != 3) {
                System.err.println("Syntax: client <coordinator> delete <tableName>");
                System.exit(1);
            }

            client.delete(args[2]);
            System.err.println("Table '" + args[2] + "' deleted");
        } else if (args[1].equals("rename")) {
            if (args.length != 4) {
                System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
                System.exit(1);
            }
            if (client.rename(args[2], args[3]))
                System.out.println("Success");
            else
                System.out.println("Failure");
        } else {
            System.err.println("Unknown command: " + args[1]);
            System.exit(1);
        }
    }
};