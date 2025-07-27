package cis5550.kvs;

import cis5550.webserver.Server;

public class Coordinator extends cis5550.generic.Coordinator {

    public static void main(String[] args) {
        // parse arguments
        if (args.length < 1) {
            System.out.println("Usage: java cis5550.kvs.Coordinator <port>");
            System.exit(1);
        }

        int port = 8000;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number");
            System.exit(1);
        }

        Server.port(port);

        registerRoutes();

        Server.get("/", (req, res) -> {
            return workerTable();
        });
    }
}
