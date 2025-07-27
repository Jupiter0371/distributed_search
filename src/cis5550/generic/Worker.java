package cis5550.generic;

import cis5550.tools.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class Worker {

    protected static volatile boolean registeredWithCoordinator = false;
    private static final Logger logger = Logger.getLogger(Worker.class);

    public static void startPingThread(String address, String id, int port) {
        // Start a thread to ping the coordinator
        new Thread(() -> {
            boolean firstPingSuccess = false;
            while (true) {
                try {
                    // construct url
                    String url = "http://" + address + "/ping?id=" + id + "&port=" + port;

                    // Ping the coordinator
                    URL obj = new URI(url).toURL();
                    obj.getContent();

                    // After the first successful ping, set the flag
                    if (!firstPingSuccess) {
                        firstPingSuccess = true;
                        registeredWithCoordinator = true;
                        logger.info("Worker registered with coordinator. ID: " + id);
                    }

                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.fatal("Ping thread interrupted: " + e.getMessage());
                    break;
                } catch (IOException e) {
                    Thread.currentThread().interrupt();
                    logger.fatal("Error pinging coordinator in startPingThread: " + e.getMessage());
                    break;
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }
}
