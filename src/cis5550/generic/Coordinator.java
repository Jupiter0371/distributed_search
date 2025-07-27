package cis5550.generic;

import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {

    private static final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private static final Logger logger = Logger.getLogger(Coordinator.class);

    private static class WorkerInfo {
        private final String id;
        private String ip;
        private String port;
        private long lastPing;

        public WorkerInfo(String id, String ip, String port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.lastPing = System.currentTimeMillis();
        }

        public void update(String ip, String port) {
            this.ip = ip;
            this.port = port;
            this.lastPing = System.currentTimeMillis();
        }
    }

    public static void registerRoutes() {
        ping();
        worker();
    }

    private static void ping() {
        Server.get("/ping", (req, res) -> {
            String workerID = req.queryParams("id");
            String workerPort = req.queryParams("port");

            if (workerID == null || workerPort == null) {
                res.status(400, "Bad Request");
                logger.error("400 Bad Request; missing parameters");
                return "Bad Request";
            }

            String workerIP = req.ip();
            WorkerInfo worker = workers.get(workerID);
            if (worker != null) {
                worker.update(workerIP, workerPort);
                logger.info("Updated worker " + workerID + " at " + workerIP + ":" + workerPort);
            } else {
                workers.put(workerID, new WorkerInfo(workerID, workerIP, workerPort));
                logger.info("Added worker " + workerID + " at " + workerIP + ":" + workerPort);
            }
            return "OK";
        });
    }

    private static void worker() {
        Server.get("/workers", (req, res) -> {
            String[] workerList = getWorkers();
            StringBuilder response = new StringBuilder();
            response.append(workerList.length).append("\n");
            for (String worker : workerList) {
                String workerID = getIDFromWorker(worker);
                if (workerID == null) {
                    logger.error("Worker ID not found for " + worker);
                    continue;
                }
                response.append(workerID).append(",").append(worker).append("\n");
            }
            return response.toString();
        });
    }

    // Return the list of workers as ip:port strings
    public static String[] getWorkers() {
        long currentTime = System.currentTimeMillis();
        List<String> workerList = new ArrayList<>();

        List<String> inactiveWorkerList = new ArrayList<>();
        for (String key : workers.keySet()) {
            WorkerInfo worker = workers.get(key);
            if ((currentTime - worker.lastPing) > 15000) {
                inactiveWorkerList.add(key);
            } else {
                workerList.add(worker.ip + ":" + worker.port);
            }
        }

//        for (String workerId : inactiveWorkerList) {
//            workers.remove(workerId);
//        }

        return workerList.toArray(new String[0]);
    }

    // Generate an HTML table of workers
    public static String workerTable() {
        StringBuilder table = new StringBuilder("<table border='1'><tr><th>ID</th><th>IP</th><th>Port</th></tr>");
        long currentTime = System.currentTimeMillis();

        List<String> inactiveWorkerList = new ArrayList<>();
        for (String key : workers.keySet()) {
            WorkerInfo worker = workers.get(key);
            if ((currentTime - worker.lastPing) > 15000) {
                inactiveWorkerList.add(key);
            } else {
                String link = "http://" + worker.ip + ":" + worker.port + "/";
                table.append("<html>").append("<tr>")
                        .append("<td>").append(worker.id).append("</td>")
                        .append("<td>").append(worker.ip).append("</td>")
                        .append("<td><a href='").append(link).append("'>").append(worker.port).append("</a></td>")
                        .append("</tr>").append("</html>");
            }
        }
        table.append("</table>");

        for (String workerId : inactiveWorkerList) {
            workers.remove(workerId);
        }

        return table.toString();
    }

    private static String getIDFromWorker(String address) {
        String[] ipPortSplit = address.split(":");
        String ip = ipPortSplit[0];
        String port = ipPortSplit[1];
        for (WorkerInfo worker : workers.values()) {
            if (worker.ip.equals(ip) && worker.port.equals(port)) {
                return worker.id;
            }
        }
        return null;
    }
}
