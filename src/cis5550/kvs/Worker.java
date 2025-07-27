package cis5550.kvs;

import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Worker extends cis5550.generic.Worker {

    private static String workerID;
    private static int workerPort = 8081;
    private static String storageDirectory;
    private static final String KEY_START = "aaaaa";
    private static final String KEY_END = "zzzzz";
    private static final String delimiter = "&";

    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, VersionedRow>> tables = new ConcurrentHashMap<>();
    private static final Logger logger = Logger.getLogger(Worker.class);

    private static class VersionedRow {
        private final ConcurrentHashMap<Integer, Row> versions = new ConcurrentHashMap<>();
        private final AtomicInteger versionCounter = new AtomicInteger(0);
        private final String rowKey;

        public VersionedRow(String rowKey) {
            this.rowKey = rowKey;
        }

        public Row getRow(int version) {
            Row row = versions.get(version);
            return row != null ? row.clone() : null;
        }

        public Row getLatestRow() {
            int latestVersion = versionCounter.get();
            if (latestVersion == 0) {
                return null;
            }
            return getRow(latestVersion);
        }

        public int getLatestVersion() {
            return versionCounter.get();
        }

        public int updateRow(Row row) {
            int version = versionCounter.incrementAndGet();
            versions.put(version, row.clone());
            return version;
        }

        public String key() {
            return rowKey;
        }

        public void printAll() {
            for (int v : versions.keySet()) {
                System.out.println("Version: " + v + " with data: " + versions.get(v).toString());
            }
        }
    }

    public static void main(String[] args) {
        // parse arguments
        if (args.length < 5) {
            System.out.println(
                    "Usage: java cis5550.kvs.Worker <port> <directory> <ip:port> <worker pos (from 0 to n - 1)> <# total workers (n)>");
            System.exit(1);
        }

        try {
            workerPort = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number: " + args[0]);
            System.exit(1);
        }
        storageDirectory = args[1];
        String coordinatorAddress = args[2];
        int workerPos = Integer.parseInt(args[3]);
        int totalWorkers = Integer.parseInt(args[4]);

        // Create storage directory if it doesn't exist
        File dir = new File(storageDirectory);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                logger.info("Created KVS storage directory: " + storageDirectory);
            } else {
                logger.fatal("Failed to create KVS storage directory: " + storageDirectory);
                System.exit(1);
            }
        } else if (!dir.isDirectory()) {
            logger.fatal("KVS Storage path exists but is not a directory: " + storageDirectory);
            System.exit(1);
        }

        // Get worker ID from storage directory
        File idFile = new File(storageDirectory, "id");
        if (idFile.exists()) {
            try {
                String fileContent = new String(Files.readAllBytes(idFile.toPath())).trim();
                if (!fileContent.isEmpty()) {
                    workerID = fileContent;
                    logger.info("KVS Worker ID retrieved from file: " + workerID);
                } else {
                    // File exists but is empty; generate new ID and write to file
                    workerID = generateWorkerID(workerPos, totalWorkers);
                    logger.info("Generated and wrote new KVS Worker ID: " + workerID);
                    Files.write(idFile.toPath(), workerID.getBytes());
                }
            } catch (IOException e) {
                logger.fatal("Error reading KVS worker ID from file: " + e.getMessage());
                System.exit(1);
            }
        } else {
            workerID = generateWorkerID(workerPos, totalWorkers);
            logger.info("Generated and wrote new KVS Worker ID: " + workerID);
            try {
                Files.write(idFile.toPath(), workerID.getBytes());
            } catch (IOException e) {
                logger.fatal("Error writing KVS worker ID to file: " + e.getMessage());
                System.exit(1);
            }
        }

        Server.port(workerPort);

        startPingThread(coordinatorAddress, workerID, workerPort);

        registerRoutes();
    }

    private static void registerRoutes() {
        getRow();
        getTable();
        getColumn();
        putTable();
        putColumn();
        renameTable();
        deleteTable();
        putBatch();

        viewHome();
        viewTable();
        viewTableNames();
        countTable();
    }

    // GET /data/:table
    private static void getTable() {
        Server.get("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            // Check if the table exists
            if (!checkTableExists(tableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            List<String> rowKeys = processRowKeys(tableName, startRow, endRowExclusive);
            if (rowKeys == null) {
                res.status(500, "Internal Server Error");
                return "Error reading row keys from the table";
            }

            List<byte[]> rowBytesList;
            if (isPersistentTable(tableName)) {
                // Handle persistent table: read from disk
                rowBytesList = rowKeys.parallelStream()
                        .map(rowKey -> readRowFromDisk(tableName, rowKey))
                        .filter(Objects::nonNull)
                        .map(Row::toByteArray)
                        .toList();
            } else {
                // Handle non-persistent table: retrieve from in-memory storage
                ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
                if (table == null) {
                    res.status(500, "Internal Server Error");
                    return "Error reading table from in-memory storage";
                }

                rowBytesList = rowKeys.parallelStream()
                        .map(rowKey -> {
                            VersionedRow versionedRow = table.get(rowKey);
                            return versionedRow != null ? versionedRow.getLatestRow() : null;
                        })
                        .filter(Objects::nonNull)
                        .map(Row::toByteArray)
                        .collect(Collectors.toList());
            }

            // Write rows sequentially to the response
            for (byte[] rowBytes : rowBytesList) {
                res.write(rowBytes);
                res.write("\n".getBytes());
            }

            res.write("\n".getBytes());
            res.header("Content-Type", "text/plain");
            return null;
        });
    }

    // GET /data/:table/:row
    private static void getRow() {
        Server.get("/data/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");

            // Check if the table exists
            if (!checkTableExists(tableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            // Retrieve the row
            VersionedRow versionedRow = getRow(tableName, rowKey);
            if (versionedRow == null || versionedRow.getLatestRow() == null) {
                res.status(404, "Not Found");
                return "Row not found";
            }

            Row row = versionedRow.getLatestRow();
            byte[] rowBytes = row.toByteArray();
            res.bodyAsBytes(rowBytes);

            return null;
        });
    }

    // GET /data/:table/:row/:column
    private static void getColumn() {
        Server.get("/data/:table/:row/:column", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");
            String columnKey = req.params("column");

            // versioning for in-memory storage
            String versionParam = req.queryParams("version");
            Integer version = null;
            if (versionParam != null) {
                try {
                    version = Integer.parseInt(versionParam);
                } catch (NumberFormatException e) {
                    res.status(404, "Not Found");
                    return "Version Not Found";
                }
            }

            VersionedRow versionedRow = getRow(tableName, rowKey);
            if (versionedRow != null) {
                Row row;
                int rowVersion;
                if (version != null) {
                    row = versionedRow.getRow(version);
                    rowVersion = version;
                } else {
                    row = versionedRow.getLatestRow();
                    rowVersion = versionedRow.getLatestVersion();
                }

                if (row != null) {
                    byte[] value = row.getBytes(columnKey);
                    if (value != null) {
                        // Add Version header
                        res.header("Version", String.valueOf(rowVersion));
                        res.bodyAsBytes(value);

                        return null;
                    }
                }
            }

            // table/row/column doesn't exist
            res.status(404, "Not Found");
            return "Row Not Found";
        });
    }

    // PUT /data/:table
    private static void putTable() {
        Server.put("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            byte[] rowBytes = req.bodyAsBytes();
            Row row = Row.readFrom(new ByteArrayInputStream(rowBytes));

            if (row == null) {
                res.status(400, "Bad Request");
                return "Invalid row data";
            }

            // Store the row
            VersionedRow versionedRow = new VersionedRow(row.key());
            versionedRow.updateRow(row);
            putRow(tableName, versionedRow);

            return "OK";
        });
    }

    // PUT /data/:table/batch (batch column processing)
    private static void putBatch() {
        Server.put("/data/:table/batch", (req, res) -> {
            String tableName = req.params("table");
            byte[] batchBytes = req.bodyAsBytes();

            if (batchBytes == null || batchBytes.length == 0) {
                res.status(400, "Bad Request");
                return "Batch data is empty";
            }

            try (ByteArrayInputStream bais = new ByteArrayInputStream(batchBytes);
                 InputStreamReader isr = new InputStreamReader(bais, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(isr)) {

                String rowLine;
                while ((rowLine = reader.readLine()) != null) {
                    String[] rowParts = rowLine.split(delimiter, 3); // rowKey, columnName, value
                    if (rowParts.length < 3) {
                        logger.error("Invalid row format: " + rowLine);
                        continue;
                    }

                    String rowKey = rowParts[0];
                    String columnName = rowParts[1];
                    String value = rowParts[2];

                    // Get or create the table
                    ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
                    if (table == null) {
                        tables.putIfAbsent(tableName, new ConcurrentHashMap<>());
                        table = tables.get(tableName);
                    }

                    // Get or create VersionedRow atomically
                    VersionedRow versionedRow = table.get(rowKey);
                    if (versionedRow == null) {
                        VersionedRow newVersionedRow = new VersionedRow(rowKey);
                        VersionedRow existing = table.putIfAbsent(rowKey, newVersionedRow);
                        versionedRow = Objects.requireNonNullElse(existing, newVersionedRow);
                    }

                    // Synchronize on the VersionedRow to prevent concurrent updates
                    synchronized (versionedRow) {
                        Row latestRow = versionedRow.getLatestRow();
                        if (latestRow == null) {
                            latestRow = new Row(rowKey);
                        }

                        latestRow.put(columnName, value.getBytes(StandardCharsets.UTF_8));
                        int version = versionedRow.updateRow(latestRow);
                        putRow(tableName, versionedRow);

                        // Add Version header (Note: This will be overwritten if multiple updates per request)
                        res.header("Version", String.valueOf(version));
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing batch PUT: ", e);
                res.status(500, "Internal Server Error");
                return "Error processing batch data";
            }

            return "OK";
        });
    }

    // PUT /data/:table/:row/:column
    private static void putColumn() {
        Server.put("/data/:table/:row/:column", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");
            String columnKey = req.params("column");
            byte[] value = req.bodyAsBytes();

            // conditional put
            String ifcolumn = req.queryParams("ifcolumn");
            String equals = req.queryParams("equals");

            VersionedRow versionedRow = getRow(tableName, rowKey);
            if (versionedRow == null) {
                versionedRow = new VersionedRow(rowKey);
            }

            Row latestRow = versionedRow.getLatestRow();
            if (latestRow == null) {
                // No existing versions; create a new Row (first time)
                latestRow = new Row(rowKey);
            }

            if (ifcolumn != null && equals != null) {
                byte[] temp = latestRow.getBytes(ifcolumn);
                if (temp == null || !new String(temp).equals(equals)) {
                    return "FAIL";
                }
            }

            // Update the row
            latestRow.put(columnKey, value);
            int version = versionedRow.updateRow(latestRow);
            putRow(tableName, versionedRow);

            // Add Version header
            res.header("Version", String.valueOf(version));

            return "OK";
        });
    }

    // GET /
    private static void viewHome() {
        Server.get("/", (req, res) -> {
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>")
                    .append("<html>")
                    .append("<head>")
                    .append("<title>Worker Tables</title>")
                    .append("<style>")
                    .append("table { border-collapse: collapse; width: 50%; }")
                    .append("th, td { border: 1px solid black; padding: 8px; text-align: left; }")
                    .append("th { background-color: #f2f2f2; }")
                    .append("</style>")
                    .append("</head>")
                    .append("<body>")
                    .append("<h1>Data Tables on Worker</h1>");

            // Get all table names (in-memory and persistent)
            Set<String> allTableNames = getAllTableNames();

            html.append("<table>")
                    .append("<tr>")
                    .append("<th>Table Name</th>")
                    .append("<th>Number of Keys</th>")
                    .append("</tr>");

            for (String tableName : allTableNames) {
                int numberOfKeys = countTableKeys(tableName);
                html.append("<tr>")
                        .append("<td><a href=\"/view/").append(tableName).append("\">")
                        .append(tableName)
                        .append("</a></td>")
                        .append("<td>").append(numberOfKeys).append("</td>")
                        .append("</tr>");
            }

            html.append("</table>")
                    .append("</body>")
                    .append("</html>");

            res.header("Content-Type", "text/html");
            return html.toString();
        });
    }

    // GET /view/:table
    private static void viewTable() {
        Server.get("/view/:table", (req, res) -> {
            String tableName = req.params("table");
            String fromRow = req.queryParams("fromRow");

            // Check if the table exists
            if (!checkTableExists(tableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            List<String> sortedRowKeys;

            if (isPersistentTable(tableName)) {
                // Handle persistent table: read from disk
                sortedRowKeys = processRowKeys(tableName, null, null);
                if (sortedRowKeys == null) {
                    res.status(500, "Internal Server Error");
                    return "Error reading row keys from the table";
                }
            } else {
                // Handle non-persistent table: retrieve from in-memory storage
                ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);

                // Get and sort the row keys
                sortedRowKeys = new ArrayList<>(table.keySet());
                Collections.sort(sortedRowKeys);
            }

            // Determine the starting index based on fromRow
            int startIndex = 0;
            if (fromRow != null && !fromRow.isEmpty()) {
                startIndex = Collections.binarySearch(sortedRowKeys, fromRow);
                if (startIndex < 0) {
                    startIndex = -startIndex - 1;
                }
            }

            // Limit to 10 rows starting from startIndex
            int endIndex = Math.min(startIndex + 10, sortedRowKeys.size());
            List<String> limitedRowKeys = sortedRowKeys.subList(startIndex, endIndex);

            // Collect unique columns from these rows
            Set<String> allColumnsSet = new TreeSet<>();
            List<Row> limitedRows = new ArrayList<>();

            for (String rowKey : limitedRowKeys) {
                Row row;
                if (isPersistentTable(tableName)) {
                    row = readRowFromDiskInParallel(tableName, rowKey);
                    if (row == null) {
                        logger.error("Error reading row from disk for table '" + tableName + "', row '" + rowKey + "'");
                        continue;
                    }
                } else {
                    VersionedRow vr = tables.get(tableName).get(rowKey);
                    row = (vr != null) ? vr.getLatestRow() : null;
                }
                if (row != null) {
                    limitedRows.add(row);
                    allColumnsSet.addAll(row.columns());
                }
            }

            // Convert the set to a sorted list
            List<String> allColumns = new ArrayList<>(allColumnsSet);

            // Start building the HTML
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>")
                    .append("<html>")
                    .append("<head>")
                    .append("<title>Table: ").append(tableName).append("</title>")
                    .append("<style>")
                    .append("table { border-collapse: collapse; width: 100%; }")
                    .append("th, td { border: 1px solid black; padding: 8px; text-align: left; }")
                    .append("th { background-color: #f2f2f2; }")
                    .append("</style>")
                    .append("</head>")
                    .append("<body>")
                    .append("<h1>Table: ").append(tableName).append("</h1>")
                    .append("<table>")
                    .append("<tr>")
                    .append("<th>Row Key</th>");

            // Add column headers
            for (String column : allColumns) {
                html.append("<th>").append(column).append("</th>");
            }
            html.append("</tr>");

            // Add rows
            for (Row row : limitedRows) {
                html.append("<tr>")
                        .append("<td>").append(row.key()).append("</td>");
                for (String column : allColumns) {
                    byte[] value = row.getBytes(column);
                    String valueStr = (value != null) ? new String(value) : "";
                    html.append("<td>").append(valueStr).append("</td>");
                }
                html.append("</tr>");
            }
            html.append("</table>");

            // Check if there is a next page
            if (endIndex < sortedRowKeys.size()) {
                String nextFromRow = sortedRowKeys.get(endIndex);
                html.append("<br>")
                        .append("<a href=\"/view/").append(tableName).append("?fromRow=")
                        .append(nextFromRow)
                        .append("\">Next</a>");
            }

            html.append("</body>")
                    .append("</html>");

            res.header("Content-Type", "text/html");
            return html.toString();
        });
    }

    // GET /tables
    private static void viewTableNames() {
        Server.get("/tables", (req, res) -> {
            // return names of all tables separated by LF
            Set<String> allTableNames = getAllTableNames();

            StringBuilder tableNames = new StringBuilder();

            for (String tableName : allTableNames) {
                tableNames.append(tableName).append("\n");
            }

            res.header("Content-Type", "text/plain");
            return tableNames.toString();
        });
    }

    // GET /count/:table
    private static void countTable() {
        Server.get("/count/:table", (req, res) -> {
            String tableName = req.params("table");

            // Check if the table exists
            if (!checkTableExists(tableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            // Retrieve the table
            if (isPersistentTable(tableName)) {
                // Handle persistent table: read from disk
                List<String> rowKeys = processRowKeys(tableName, null, null);
                if (rowKeys == null) {
                    res.status(500, "Internal Server Error");
                    return "Error reading row keys from table directory";
                }

                return rowKeys.size();
            } else {
                // Handle non-persistent table: retrieve from in-memory storage
                ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
                return table.size();
            }
        });
    }

    // PUT /rename/:table
    private static void renameTable() {
        Server.put("/rename/:table", (req, res) -> {
            String oldTableName = req.params("table");
            String newTableName = req.body();

            // Check if new table name is empty
            if (newTableName == null || newTableName.isEmpty()) {
                res.status(404, "Not Found");
                return "New table name is empty";
            }

            // Check if old table exists
            if (!checkTableExists(oldTableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            // Check if new table name already exists
            if (checkTableExists(newTableName)) {
                res.status(409, "Conflict");
                return "Table with the new name already exists";
            }

            // Check naming convention for persistent tables
            if (oldTableName.startsWith("pt-") && !newTableName.startsWith("pt-")) {
                res.status(400, "Bad Request");
                return "Persistent tables must start with 'pt-'";
            }

            // Perform the rename operation
            boolean success = renameTableProcess(oldTableName, newTableName);
            if (success) {
                return "OK";
            } else {
                res.status(500, "Internal Server Error");
                return "Failed to rename table";
            }
        });
    }

    // PUT /delete/:table
    private static void deleteTable() {
        Server.put("/delete/:table", (req, res) -> {
            String tableName = req.params("table");

            // Check if table exists
            if (!checkTableExists(tableName)) {
                res.status(404, "Not Found");
                return "Table not found";
            }

            // Perform the delete operation
            boolean success = deleteTableProcess(tableName);
            if (success) {
                return "OK";
            } else {
                res.status(500, "Internal Server Error");
                return "Failed to delete table";
            }
        });
    }

    private static VersionedRow getRow(String tableName, String rowKey) {
        if (isPersistentTable(tableName)) {
            // Handle persistent table: read from disk
            Row row = readRowFromDiskInParallel(tableName, rowKey);
            if (row != null) {
                VersionedRow versionedRow = new VersionedRow(rowKey);
                versionedRow.updateRow(row);
                return versionedRow;
            } else {
                return null;
            }
        } else {
            // Handle non-persistent table: retrieve from in-memory storage
            ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
            if (table != null) {
                return table.get(rowKey);
            } else {
                return null;
            }
        }
    }

    private static void putRow(String tableName, VersionedRow versionedRow) {
        if (isPersistentTable(tableName)) {
            // Handle persistent table: write directly to disk
            Row latestRow = versionedRow.getLatestRow();
            if (latestRow != null) {
                writeRowToDisk(tableName, latestRow);
            } else {
                logger.error("Error writing row to disk for table '" + tableName + "': latest row is null");
            }
        } else {
            // Handle non-persistent table: existing in-memory storage
            tables.putIfAbsent(tableName, new ConcurrentHashMap<>());
            ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
            table.put(versionedRow.key(), versionedRow);
        }
    }

    private static Row readRowFromDisk(String tableName, String rowKey) {
        try {
            // Define the path to the table's subdirectory
            Path tableDir = Paths.get(storageDirectory, tableName);

            // Encode the row key to create a valid filename
            String encodedRowKey = KeyEncoder.encode(rowKey);
            Path rowFile = tableDir.resolve(encodedRowKey);

            // Check if the row file exists
            if (!Files.exists(rowFile)) {
                // logger.info("Row file does not exist: " + rowFile);
                return null;
            }

            // Read all bytes from the row file
            byte[] data = Files.readAllBytes(rowFile);

            // Deserialize the Row object from the byte array
            return Row.readFrom(new ByteArrayInputStream(data));
        } catch (IOException e) {
            logger.error("IO Error reading row from disk for table '"
                    + tableName + "', row '" + rowKey + "': " + e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("Deserialization Error reading row from disk for table '"
                    + tableName + "', row '" + rowKey + "': " + e.getMessage());
            return null;
        }
    }

    private static Row readRowFromDiskInParallel(String tableName, String rowKey) {
        try {
            // Define the path to the table's subdirectory
            Path tableDir = Paths.get(storageDirectory, tableName);

            // Use a stream to find the matching row file
            try (Stream<Path> rowFiles = Files.list(tableDir)) {
                return rowFiles.parallel()
                        .filter(rowFile -> rowFile.getFileName().toString().equals(KeyEncoder.encode(rowKey)))
                        .findFirst()
                        .map(rowFile -> {
                            try {
                                byte[] data = Files.readAllBytes(rowFile);
                                return Row.readFrom(new ByteArrayInputStream(data));
                            } catch (IOException e) {
                                logger.error("IO Error reading row file for table '" + tableName + "', rowKey '"
                                        + rowKey + "': " + e.getMessage());
                                return null;
                            } catch (Exception e) {
                                logger.error("Deserialization error for row file in table '" + tableName
                                        + "', rowKey '" + rowKey + "': " + e.getMessage());
                                return null;
                            }
                        })
                        .orElseGet(() -> {
                            logger.info("No matching row file found for rowKey: " + rowKey);
                            return null;
                        });
            }
        } catch (IOException e) {
            logger.error("IO Error listing files in table directory '" + tableName + "': " + e.getMessage());
            return null;
        }
    }

    private static void writeRowToDisk(String tableName, Row row) {
        try {
            // Write to subdirectory
            Path tableDir = Paths.get(storageDirectory, tableName);

            // Create the table directory if it doesn't exist
            if (!Files.exists(tableDir)) {
                Files.createDirectories(tableDir);
                logger.info("Created directory for persistent table: " + tableDir);
            }

            // Encode the row key to create a valid filename
            String encodedRowKey = KeyEncoder.encode(row.key());
            Path rowFile = tableDir.resolve(encodedRowKey);

            // Write the serialized Row to the file
            Files.write(rowFile, row.toByteArray());
            logger.info("Wrote/Updated row file: " + rowFile);
        } catch (IOException e) {
            logger.error("Error writing row to disk for table '" + tableName + "': " + e.getMessage());
        }
    }

    private static List<String> processRowKeys(String tableName, String startRow, String endRowExclusive) {
        List<String> rowKeys = new ArrayList<>();

        if (isPersistentTable(tableName)) {
            Path tableDir = Paths.get(storageDirectory, tableName);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir)) {
                for (Path file : stream) {
                    try {
                        String rowKey = KeyEncoder.decode(file.getFileName().toString());
                        rowKeys.add(rowKey);
                    } catch (Exception e) {
                        logger.error("Error decoding row key from file: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                logger.error("Error reading row keys from table directory: " + e.getMessage());
                return null;
            }
        } else {
            ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
            if (table != null) {
                rowKeys.addAll(table.keySet());
            } else {
                logger.error("Error reading row keys from the in-memory storage");
                return null;
            }
        }

        // Sort the row keys
        Collections.sort(rowKeys);

        // Filter the row keys
        if (startRow != null) {
            int startIndex = Collections.binarySearch(rowKeys, startRow);
            if (startIndex < 0) {
                startIndex = -startIndex - 1;
            }
            rowKeys = rowKeys.subList(startIndex, rowKeys.size());
        }

        if (endRowExclusive != null) {
            int endIndex = Collections.binarySearch(rowKeys, endRowExclusive);
            if (endIndex < 0) {
                endIndex = -endIndex - 1;
            }
            rowKeys = rowKeys.subList(0, endIndex);
        }

        return rowKeys;
    }

    private static boolean renameTableProcess(String oldName, String newName) {
        try {
            if (isPersistentTable(oldName)) {
                // Encode table names
                String encodedOldName = KeyEncoder.encode(oldName);
                String encodedNewName = KeyEncoder.encode(newName);

                // Construct paths
                Path oldDir = Paths.get(storageDirectory, encodedOldName);
                Path newDir = Paths.get(storageDirectory, encodedNewName);

                logger.info("Renaming persistent table from '" + oldDir + "' to '" + newDir + "'");

                // Check if old directory exists
                if (!Files.exists(oldDir)) {
                    logger.error("Old table directory does not exist: " + oldDir);
                    return false;
                }

                // Check if new directory already exists
                if (Files.exists(newDir)) {
                    logger.error("New table directory already exists: " + newDir);
                    return false;
                }

                // Perform the move operation
                Files.move(oldDir, newDir);

                // Verify the move
                if (Files.exists(newDir) && !Files.exists(oldDir)) {
                    logger.info("Successfully renamed persistent table directory");
                    return true;
                } else {
                    logger.error("Failed to rename persistent table directory");
                    return false;
                }
            } else {
                // Rename the in-memory table
                ConcurrentHashMap<String, VersionedRow> tableData = tables.remove(oldName);
                if (tableData != null) {
                    // If the new table name starts with 'pt-', persist the table to disk
                    if (isPersistentTable(newName)) {
                        // Create the new directory
                        String encodedNewName = KeyEncoder.encode(newName);
                        Path newDir = Paths.get(storageDirectory, encodedNewName);
                        Files.createDirectories(newDir);

                        // Save the table data to disk
                        for (Map.Entry<String, VersionedRow> entry : tableData.entrySet()) {
                            String rowKey = entry.getKey();
                            VersionedRow row = entry.getValue();
                            Row latestRow = row.getLatestRow();
                            if (latestRow != null) {
                                writeRowToDisk(newName, latestRow);
                            }
                        }

                        logger.info("Persisted in-memory table to disk: " + newName);
                    } else {
                        // Add the table data to in-memory storage
                        tables.put(newName, tableData);
                        logger.info("Renamed table in in-memory storage: " + newName);
                    }

                    return true;
                } else {
                    return false;
                }
            }
        } catch (IOException e) {
            logger.error("Error renaming table from '" + oldName + "' to '" + newName + "': " + e.getMessage());
            return false;
        }
    }

    private static boolean deleteTableProcess(String tableName) {
        try {
            if (isPersistentTable(tableName)) {
                // Delete the directory and all its contents
                Path tableDir = Paths.get(storageDirectory, tableName);
                if (Files.exists(tableDir)) {
                    // Delete all files in the directory
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir)) {
                        for (Path file : stream) {
                            Files.delete(file);
                        }
                    } catch (IOException e) {
                        logger.error("Error deleting files in table directory: " + e.getMessage());
                        return false;
                    }
                    // Delete the directory itself
                    Files.delete(tableDir);
                    logger.info("Deleted persistent table directory: " + tableDir);
                }

                // Remove the in-memory table
                tables.remove(tableName);
            } else {
                // Remove the in-memory table
                tables.remove(tableName);
                logger.info("Deleted table from in-memory storage: " + tableName);
            }
            return true;
        } catch (IOException e) {
            logger.error("Error deleting table '" + tableName + "': " + e.getMessage());
            return false;
        }
    }

    /***** Helper methods *****/

    private static String generateRandomID() {
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append((char) (Math.random() * 26 + 97));
        }
        return id.toString();
    }

    private static String generateWorkerID(int position, int totalWorkers) {
        if (position < 0 || position >= totalWorkers) {
            logger.error("Invalid KVS worker position: " + position);
            throw new IllegalArgumentException("Invalid totalWorkers or position.");
        }

        // Convert keys to numeric space
        BigInteger startValue = convertToNumber(KEY_START);
        BigInteger endValue = convertToNumber(KEY_END);
        BigInteger totalRange = endValue.subtract(startValue).add(BigInteger.ONE);

        // Compute evenly spaced positions
        BigInteger step = totalRange.divide(BigInteger.valueOf(totalWorkers));
        BigInteger workerPosition = startValue.add(step.multiply(BigInteger.valueOf(position)))
                .add(step.divide(BigInteger.valueOf(2)));

        // Convert numeric position to string
        return convertToString(workerPosition);
    }

    private static BigInteger convertToNumber(String id) {
        BigInteger value = BigInteger.ZERO;
        for (char c : id.toCharArray()) {
            value = value.multiply(BigInteger.valueOf(26));
            value = value.add(BigInteger.valueOf(c - 'a'));
        }
        return value;
    }

    private static String convertToString(BigInteger value) {
        char[] chars = new char[5];
        for (int i = 4; i >= 0; i--) {
            BigInteger[] divRem = value.divideAndRemainder(BigInteger.valueOf(26));
            chars[i] = (char) ('a' + divRem[1].intValue());
            value = divRem[0];
        }
        return new String(chars);
    }

    private static boolean isPersistentTable(String tableName) {
        return tableName.startsWith("pt-");
    }

    private static boolean checkTableExists(String tableName) {
        if (isPersistentTable(tableName)) {
            Path tableDir = Paths.get(storageDirectory, tableName);
            return Files.exists(tableDir);
        } else {
            return tables.containsKey(tableName);
        }
    }

    private static int countTableKeys(String tableName) {
        if (isPersistentTable(tableName)) {
            List<String> rowKeys = processRowKeys(tableName, null, null);
            return (rowKeys != null) ? rowKeys.size() : 0;
        } else {
            ConcurrentHashMap<String, VersionedRow> table = tables.get(tableName);
            return (table != null) ? table.size() : 0;
        }
    }

    private static Set<String> getAllTableNames() {
        // Get all table names from in-memory storage
        Set<String> allTableNames = new TreeSet<>(tables.keySet());
        // Get all table names from persistent storage
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(storageDirectory))) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    allTableNames.add(path.getFileName().toString());
                }
            }
        } catch (IOException e) {
            logger.error("Error reading storage directory: " + e.getMessage());
        }
        return allTableNames;
    }
}
