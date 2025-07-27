package cis5550.webserver;

import cis5550.tools.Logger;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.CertificateException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class Server {

    public static Server instance = null;
    public static boolean isRunning = false;
    private int port = 8080;
    private int securePort = -1;
    public boolean isSecure = false;
    private String root = "";

    // for logging
    private static final Logger logger = Logger.getLogger(Server.class);

    // for thread pool
    private static final int NUM_WORKERS = 1000;
    private static final BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<>(NUM_WORKERS);

    // for multiple hosts
    private final Map<String, List<RouteInput>> hostRoutes = new HashMap<>();
    private final List<RouteInput> defaultRoutes = new ArrayList<>();
    private String currentHost = null;

    // for filters
    private final List<RouteFilter> beforeFilters = new ArrayList<>();
    private final List<RouteFilter> afterFilters = new ArrayList<>();

    // for sessions
    public final Map<String, Session> sessions = new HashMap<>();

    /**
     * Start the server
     */
    public void run() {
        if (instance.root.isEmpty()) {
            logger.info("Root directory not set");
        }

        // initialize workers
        for (int i = 0; i < NUM_WORKERS; i++) {
            new Thread(new Worker(socketQueue, instance.root)).start();
        }

        // initialize session checker
        SessionChecker.start(sessions);

        // start the http server
        new Thread(() -> httpServer(instance.port, instance.root)).start();

        // start the https server
        if (instance.securePort != -1) {
            instance.isSecure = true;
            new Thread(() -> httpsServer(instance.securePort, instance.root)).start();
        }
    }

    /**
     * Start a https server. The server only starts if the secure port is set
     * @param port the port number
     * @param root the root directory of the files
     */
    private void httpsServer(int port, String root) {
        ServerSocketFactory factory;
        try {
            // initialize the secure socket
            String pwd = "secret";
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, pwd.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            factory = sslContext.getServerSocketFactory();
        } catch (UnrecoverableKeyException | CertificateException | KeyStoreException | IOException |
                 NoSuchAlgorithmException | KeyManagementException e) {
            logger.fatal("Error initializing secure socket", e);
            throw new RuntimeException(e);
        }

        try (ServerSocket ssock = factory.createServerSocket(securePort)) {
            logger.info("Server started on https port " + port);
            logger.info("Root directory of the files: " + root);

            while (true) {
                Socket sock = ssock.accept();
                logger.info("Connection from: " + sock.getRemoteSocketAddress());

                // add the socket to the queue
                socketQueue.put(sock);
            }
        } catch (IOException e) {
            logger.fatal("Error starting server on port " + port, e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Error putting socket into queue", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Start a http server
     * @param port the port number
     * @param root the root directory of the files
     */
    private void httpServer(int port, String root) {
        try (ServerSocket ssock = new ServerSocket(port)) {
            logger.info("Server started on http port " + port);
            logger.info("Root directory of the files: " + root);

            while (true) {
                Socket sock = ssock.accept();
                logger.info("Connection from: " + sock.getRemoteSocketAddress());

                // add the socket to the queue
                socketQueue.put(sock);
            }
        } catch (IOException e) {
            logger.fatal("Error starting server on port " + port, e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Error putting socket into queue", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Keep reading from the client socket until the client closes the connection
     * @param sock the socket
     * @param root the root directory of the files
     * @throws IOException if an I/O error occurs
     */
    private void client(Socket sock, String root) throws IOException {
        ByteArrayOutputStream socketInput = new ByteArrayOutputStream();
        InputStream in = sock.getInputStream();
        int b;
        int headerTimeout = 100000; // milliseconds TODO: choose a good one?

        long startTime = System.currentTimeMillis();

        // double CRLF check
        int checkStart = 0;
        byte[] doubleCRLF = new byte[]{13, 10, 13, 10};

        // read the input from the client until the client closes
        while ((b = in.read()) != -1) {
            socketInput.write(b);

            // check for double CRLF
            if (b == doubleCRLF[checkStart]) {
                checkStart++;
                if (checkStart == 4) {
                    // find a header; start to process the request
                    processClient(sock, socketInput, in, root);

                    // ready for the next client input
                    checkStart = 0;
                    socketInput.reset();
                    startTime = System.currentTimeMillis();
                }
            } else {
                checkStart = 0;
            }

            // check for header timeout
            // long elapsedTime = System.currentTimeMillis() - startTime;
            // if (elapsedTime > headerTimeout) {
            //     logger.error("Header timeout");
            //     break;
            // }
        }

        // close the socket after the client closes
        sock.close();
    }

    /**
     * The main method to process the client request and send the response
     * @param sock the socket
     * @param socketInput the byte array of the socket input
     * @param in the input stream
     * @param root the root directory of the files
     * @throws IOException if an I/O error occurs
     */
    private void processClient(Socket sock, ByteArrayOutputStream socketInput, InputStream in, String root) throws IOException {
        // convert the byte array to string and start to read
        ByteArrayInputStream requestStream = new ByteArrayInputStream(socketInput.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(requestStream));

        // read the request
        String[] requestParts = parseRequest(reader.readLine());
        if (requestParts == null) {
            sendErrResponse(sock, 400);
            return;
        }

        // validate method
        if (!requestParts[0].equals("GET") && !requestParts[0].equals("POST")
                && !requestParts[0].equals("HEAD") && !requestParts[0].equals("PUT")) {
            sendErrResponse(sock, 501);
            return;
        }

        // validate HTTP protocol
        if (!requestParts[2].equals("HTTP/1.1")) {
            logger.error("Invalid HTTP protocol: " + requestParts[2]);
            sendErrResponse(sock, 505);
            return;
        }

        logger.info("Finish reading request: " + requestParts[0] + " " + requestParts[1] + " " + requestParts[2]);

        // read the headers and store them in a map
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String line = reader.readLine();

        while (line != null) {
            if (line.isEmpty()) break;

            String[] headerParts = line.split(": ");
            if (headerParts.length != 2) {
                logger.error("Invalid header: " + line);
                sendErrResponse(sock, 400);
                return;
            }

            headers.put(headerParts[0], headerParts[1]);

            line = reader.readLine();
        }

        // read host header
        if (!headers.containsKey("Host")) {
            logger.error("Host header not found");
            sendErrResponse(sock, 400);
            return;
        }
        // ignore port if exists
        String host = headers.get("Host").split(":")[0];

        // read cookie header (only takes sessionID)
        Session session = null; // if session remains null, no session is provided
        if (headers.containsKey("Cookie")) {
            String[] cookies = headers.get("Cookie").split("; ");
            for (String cookie : cookies) {
                String[] curr = cookie.split("=");
                if (curr.length != 2) {
                    logger.error("Invalid cookie format: " + cookie);
                    sendErrResponse(sock, 400);
                    return;
                }

                if (curr[0].equals("SessionID")) {
                    session = sessions.get(curr[1]);
                    // if a session id is provided but not found, continue
                    if (session == null) {
                        return;
                    }

                    // update last accessed time if session is found
                    if (!((SessionImpl) session).checkExpired()) {
                        ((SessionImpl) session).setLastAccessedTime(System.currentTimeMillis());
                    } else {
                        // remove session
                        sessions.remove(curr[1]);
                        session = null;
                    }
                }
            }
        }

        // read the body
        byte[] body = null;
        StringBuilder bodyString = null;
        if (headers.containsKey("Content-Length")) {
            int contentLength;
            try {
                contentLength = Integer.parseInt(headers.get("Content-Length"));
            } catch (Exception e) {
                logger.error("Invalid content length format (not an int)");
                sendErrResponse(sock, 400);
                return;
            }

            if (contentLength < 0) {
                logger.error("Invalid content length (less than 0)");
                sendErrResponse(sock, 400);
                return;
            }

            body = new byte[contentLength];
            int totalBytesRead = 0;
            int bytesRead = 0;

            while (totalBytesRead < contentLength) {
                bytesRead = in.read(body, totalBytesRead, contentLength - totalBytesRead);
                if (bytesRead == -1) {
                    logger.error("Unexpected end of stream");
                    sendErrResponse(sock, 400);
                    return;
                }
                totalBytesRead += bytesRead;
            }

            // process the body
            ByteArrayInputStream bodyStream = new ByteArrayInputStream(body);
            BufferedReader bodyReader = new BufferedReader(new InputStreamReader(bodyStream));

            // the message body is not being used
            bodyString = new StringBuilder();
            String bodyLine = bodyReader.readLine();

            while ((bodyLine != null)) {
                bodyString.append(bodyLine);
                bodyLine = bodyReader.readLine();
            }
        }

        logger.info("Finish reading headers and body");

        // find the query parameters
        Map<String, String> queries = findQueryParamsFromPath(requestParts[1]);
        if (headers.containsKey("Content-Type")
                && headers.get("Content-Type").equals("application/x-www-form-urlencoded")
                && bodyString != null) {
            Map<String, String> bodyParams = findQueryParamsFromBody(bodyString);
            queries.putAll(bodyParams);
        }

        logger.info("Found query parameters: " + queries);

        // check host specific routes
        Route route = null;
        Map<String, String> params = null;
        List<RouteInput> hostRoutes = instance.hostRoutes.get(host);
        if (hostRoutes != null) {
            // check host routes
            for (RouteInput routeInput : hostRoutes) {
                if (routeInput.method.equals(requestParts[0])) {
                    params = matchRoutePath(requestParts[1], routeInput.path);
                    if (params != null) {
                        route = routeInput.route;
                        break;
                    }
                }
            }
        } else {
            // check default routes
            for (RouteInput routeInput : instance.defaultRoutes) {
                if (routeInput.method.equals(requestParts[0])) {
                    params = matchRoutePath(requestParts[1], routeInput.path);
                    if (params != null) {
                        route = routeInput.route;
                        break;
                    }
                }
            }
        }

        if (route != null) {
            ResponseImpl res = new ResponseImpl(sock);
            Request req = new RequestImpl(requestParts[0],
                    requestParts[1],
                    requestParts[2],
                    headers,
                    queries,
                    params,
                    (InetSocketAddress) sock.getRemoteSocketAddress(),
                    body,
                    instance,
                    session,
                    res);

            try {
                Object handler = route.handle(req, res);
                logger.info("Handling route: " + requestParts[0] + " " + requestParts[1] + " " + requestParts[2]);

                // send the response
                sendRouteResponse(sock, res, handler, req);

                return;
            } catch (Exception e) {
                logger.error("Error handling route", e);
                // if write is called, close the connection only
                if (res.checkWriteCalled()) {
                    sock.close();
                } else {
                    sendErrResponse(sock, 500);
                }
                return;
            }
        }

        logger.info("Processing static file request: " + requestParts[1]);

        if (root.isEmpty()) {
            logger.error("Root directory not set");
            sendErrResponse(sock, 404);
            return;
        }

        // cut the query parameters
        String rawPath = requestParts[1].split("\\?")[0];
        String path = root + rawPath;

        File f = new File(path);

        if (requestParts[1].contains("..")) {
            logger.error("Invalid path: " + path);
            sendErrResponse(sock, 403);
            return;
        }

        if (!f.isFile()) {
            logger.error("File not found: " + path);
            sendErrResponse(sock, 404);
            return;
        }

        if (!Files.isReadable(f.toPath())) {
            logger.error("File not readable: " + path);
            sendErrResponse(sock, 403);
            return;
        }

        // for If-Modified-Since header
        if (headers.containsKey("If-Modified-Since")) {
            String lastModifiedLimit = headers.get("If-Modified-Since");
            if (lastModifiedLimit == null || lastModifiedLimit.isEmpty()) {
                logger.error("Invalid If-Modified-Since header");
                sendErrResponse(sock, 400);
                return;
            }

            // check if the file is modified since the time limit
            ZonedDateTime zdt = ZonedDateTime.parse(lastModifiedLimit, DateTimeFormatter.RFC_1123_DATE_TIME);
            long lastModified = f.lastModified();
            Instant instant = Instant.ofEpochMilli(lastModified);
            ZonedDateTime lastModifiedTime = ZonedDateTime.ofInstant(instant, zdt.getZone());
            // if the file is not modified since the time limit, return 304
            if (!lastModifiedTime.isAfter(zdt)) {
                logger.info("File not modified since the time limit: " + path);
                sendErrResponse(sock, 304);
                return;
            }
        }

        sendStaticFileResponse(sock, path, requestParts[0], f);
    }

    /**
     * Given a request, parse the request into method, path, and protocol
     * @param request the request string
     * @return an array of the request parts
     */
    private String[] parseRequest(String request) {
        if (request == null || request.isEmpty()) {
            logger.error("Empty request");
            return null;
        }

        String[] requestParts = request.split(" ");
        if (requestParts.length != 3) {
            logger.error("Invalid request (less than 3 arguments " + request);
            return null;
        }

        return requestParts;
    }

    /**
     * Send an error response to the client
     * @param sock the socket
     * @param code the error code
     * @throws IOException if an I/O error occurs
     */
    private void sendErrResponse(Socket sock, int code) throws IOException {
        PrintWriter out = new PrintWriter(sock.getOutputStream());
        logger.info("Sending response with error code: " + code);

        // Send HTTP status and headers
        if (code == 400) {
            out.print("HTTP/1.1 400 Bad Request\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 15\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("400 Bad Request");
        } else if (code == 401) {
            out.print("HTTP/1.1 401 Not allowed\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 15\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("401 Not allowed");
        } else if (code == 403) {
            out.print("HTTP/1.1 403 Forbidden\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 13\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("403 Forbidden");
        } else if (code == 404) {
            out.print("HTTP/1.1 404 Not Found\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 13\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("404 Not Found");
        } else if (code == 405) {
            out.print("HTTP/1.1 405 Not Allowed\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 15\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("405 Not Allowed");
        } else if (code == 500) {
            out.print("HTTP/1.1 500 Internal Server Error\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 25\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("500 Internal Server Error");
        } else if (code == 501) {
            out.print("HTTP/1.1 501 Not Implemented\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 19\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("501 Not Implemented");
        } else if (code == 505) {
            out.print("HTTP/1.1 505 HTTP Version Not Supported\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("Content-Length: 30\r\n");
            out.print("\r\n");

            // Send the response body
            out.print("505 HTTP Version Not Supported");
        } else if (code == 304) {
            out.print("HTTP/1.1 304 Not Modified\r\n");
            out.print("Server: CIS5550\r\n");
            out.print("\r\n");

            // according to HTTP rules, 304 should not have a body
        }

        // Flush the output
        out.flush();
    }

    /**
     * Send a static file response to the client
     * @param sock the socket
     * @param path the path of the file
     * @param method the request method
     * @param f the file object
     * @throws IOException if an I/O error occurs
     */
    private void sendStaticFileResponse(Socket sock, String path, String method, File f) throws IOException {
        PrintWriter out = new PrintWriter(sock.getOutputStream());
        logger.info("Sending static file response with response code: " + 200);

        // Send HTTP status code
        out.print("HTTP/1.1 200 OK\r\n");

        // Send the content type
        if (path.endsWith(".html")) {
            out.print("Content-Type: text/html\r\n");
        } else if (path.endsWith((".jpg")) || path.endsWith(".jpeg")) {
            out.print("Content-Type: image/jpeg\r\n");
        } else if (path.endsWith(".txt")) {
            out.print("Content-Type: text/plain\r\n");
        } else if (path.endsWith(".css")) {
            out.print("Content-Type: text/css\r\n");
        } else if (path.endsWith(".js")) {
            out.print("Content-Type: application/javascript\r\n");
        } else {
            out.print("Content-Type: application/octet-stream\r\n");
        }

        // Send the server name
        out.print("Server: CIS5550\r\n");

        // Send the content length
        long byteLength = f.length();
        out.print("Content-Length: " + byteLength + "\r\n");

        // Send the second CRLF to end the header
        out.print("\r\n");

        // Head request does not need to send the message
        if (method.equals("HEAD")) {
            out.flush();
            return;
        }

        // Send the response body
        out.flush();
        try (FileInputStream fileIn = new FileInputStream(f)) {
            OutputStream outStream = sock.getOutputStream();
            byte[] buf = new byte[1024];
            int count;

            while ((count = fileIn.read(buf)) != -1) {
                outStream.write(buf, 0, count);
            }

        } catch (IOException e) {
            logger.error("Error sending file while sending response: " + path, e);
            return;
        }

        out.flush();
    }

    /**
     * Send a route response to the client
     * @param sock the socket
     * @param res the response object
     * @param handler the handler object
     * @param req the request object
     * @throws IOException if an I/O error occurs
     */
    public void sendRouteResponse(Socket sock, ResponseImpl res, Object handler, Request req) throws IOException {
        PrintWriter out = new PrintWriter(sock.getOutputStream());

//        // process before filters
//        beforeProcess(req, res);
//        if (res.checkHalted()) {
//            try {
//                res.writeHaltResponse();
//            } catch (Exception e) {
//                logger.error("Error writing halt response", e);
//            }
//            return;
//        }

        // determine the response body
        byte[] body;

        if (res.checkWriteCalled()) {
            // close the connection if write has been called
            out.close();
            sock.close();
            return;
        } else if (handler != null) {
            body = handler.toString().getBytes();
        } else {
            body = res.getBody();
        }

        // Send HTTP status code
        out.print("HTTP/1.1 " + res.getStatusCode() + " " + res.getStatusPhrase() + "\r\n");

        // Send the content type (default to text/html)
        out.print("Content-Type: " + res.getContentType() + "\r\n");

        // Send the server name
        out.print("Server: CIS5550\r\n");

        // Send the content length (by first add to res)
        if (body != null) {
            res.header("Content-Length", Integer.toString(body.length));
        } else {
            // if no body message, set content length to 0
            res.header("Content-Length", "0");
        }

        // Add other headers
        for (String key : res.getHeaders().keySet()) {
            List<String> values = res.getHeaders().get(key);
            for (String value : values) {
                out.print(key + ": " + value + "\r\n");
            }
        }

        // Send the second CRLF to end the header
        out.print("\r\n");

        // Head request does not need to send the message
        if (req.requestMethod().equals("HEAD")) {
            out.flush();
            return;
        }

        // Send the response body
        out.flush();
        if (body != null) {
            sock.getOutputStream().write(body);
        }

        out.flush();

        logger.info("Sent route response with response code: " + res.getStatusCode());

//        // process after filters
//        afterProcess(req, res);
    }

    /**
     * Match the URL path with the route path
     * @param URLpath the URL path (from the request)
     * @param RoutePath the route path (from the route)
     * @return a map of the parameters
     */
    private Map<String, String> matchRoutePath(String URLpath, String RoutePath) {
        // if URL contains query parameters, remove them
        int start = URLpath.indexOf("?");
        if (start != -1) {
            URLpath = URLpath.substring(0, start);
        }

        // Split by '/'
        String[] URLparts = URLpath.split("/");
        String[] RouteParts = RoutePath.split("/");

        if (URLparts.length != RouteParts.length) {
            return null;
        }

        Map<String, String> params = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (int i = 0; i < URLparts.length; i++) {
            if (RouteParts[i].startsWith(":")) {
                // remove the colon and add to the params
                params.put(RouteParts[i].substring(1), URLparts[i]);
            } else if (!RouteParts[i].equals(URLparts[i])) {
                return null;
            }
        }

        return params;
    }

    /**
     * Find the query parameters from the path
     * @param path the path from the request
     * @return a map of the query parameters
     */
    private Map<String, String> findQueryParamsFromPath(String path) {
        if (path == null || path.isEmpty()) {
            return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        Map<String, String> params = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        // get the start index
        int start = path.indexOf("?");
        if (start != -1 && start < path.length() - 1) {
            String query = path.substring(start + 1);
            String[] pairs = query.split("&");

            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2); // Limit to 2 parts
                if (keyValue.length != 2) {
                    logger.error("Invalid query parameter: " + pair);
                    continue;
                }

                String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
                String value = "";
                if (keyValue.length > 1) {
                    value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                }
                params.put(key, value);
            }
        }

        return params;
    }

    /**
     * Find the query parameters from the body when the content type is application/x-www-form-urlencoded
     * @param bodyString the body in string
     * @return a map of the query parameters
     */
    private Map<String, String> findQueryParamsFromBody(StringBuilder bodyString) {
        if (bodyString == null || bodyString.isEmpty()) {
            return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        Map<String, String> params = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        String[] pairs = bodyString.toString().split("&");

        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length != 2) {
                logger.error("Invalid body parameter: " + pair);
                continue;
            }

            try {
                String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
                String value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                params.put(key, value);
            } catch (Exception e) {
                logger.error("Error decoding body parameter", e);
            }
        }

        return params;
    }

    /**
     * Execute the before filters
     * @param req the request
     * @param res the response
     */
    private void beforeProcess(Request req, Response res) {
        for (RouteFilter filter : beforeFilters) {
            try {
                filter.handle(req, res);
            } catch (Exception e) {
                logger.error("Error processing before filter", e);
            }
        }
    }

    /**
     * Execute the after filters
     * @param req the request
     * @param res the response
     */
    private void afterProcess(Request req, Response res) {
        for (RouteFilter filter : afterFilters) {
            try {
                filter.handle(req, res);
            } catch (Exception e) {
                logger.error("Error processing after filter", e);
            }
        }
    }

    /**
     * Add a route to the list of stored routes
     * @param method the request method
     * @param path the request path
     * @param route the route object
     */
    private void addRoute(String method, String path, Route route) {
        if (instance.currentHost != null) {
            instance.hostRoutes.get(instance.currentHost).add(new RouteInput(method, path, route));
        } else {
            instance.defaultRoutes.add(new RouteInput(method, path, route));
        }
    }

    ///************ SubClasses ************///

    /**
     * A worker thread to process the client request
     */
    private class Worker implements Runnable {
        private final BlockingQueue<Socket> socketQueue;
        private final String root;

        public Worker(BlockingQueue<Socket> socketQueue, String root) {
            this.socketQueue = socketQueue;
            this.root = root;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket sock = socketQueue.take();
                    client(sock, root);
                } catch (InterruptedException e) {
                    logger.error("Error taking socket from queue", e);
                } catch (IOException e) {
                    logger.warn("Socket closed");
                }
            }
        }
    }

    /**
     * A session checker to check the session expiration
     */
    private static class SessionChecker implements Runnable {
        private final Map<String, Session> sessions;

        public SessionChecker(Map<String, Session> sessions) {
            this.sessions = sessions;
        }

        @Override
        public void run() {
            for (String key : sessions.keySet()) {
                SessionImpl session = (SessionImpl) sessions.get(key);
                if (session.checkExpired()) {
                    // delete the session if expired
                    sessions.remove(key);
                }
            }
        }

        public static void start(Map<String, Session> sessions) {
            SessionChecker checker = new SessionChecker(sessions);
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            try {
                executor.scheduleAtFixedRate(checker, 0, 5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Error starting the scheduled session checker", e);
            } finally {
                if (!executor.isShutdown()) {
                    executor.shutdown();
                }
            }
        }
    }

    /**
     * initialize a route input when adding a route
     */
    private static class RouteInput {
        String method;
        String path;
        Route route;

        public RouteInput(String method, String path, Route route) {
            this.method = method;
            this.path = path;
            this.route = route;
        }
    }

    /**
     * The route filter for before/after filters
     */
    public interface RouteFilter {
        void handle(Request req, Response res) throws Exception;
    }

    ///************ Static Methods (API) ************///

    /**
     * Add a GET route
     * @param s the route path
     * @param route the route object
     */
    public static void get(String s, Route route) {
        if (instance == null) {
            instance = new Server();
        }

        instance.addRoute("GET", s, route);
        logger.info("Added route: GET " + s);

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }

    /**
     * Add a POST route
     * @param s the route path
     * @param route the route object
     */
    public static void post(String s, Route route) {
        if (instance == null) {
            instance = new Server();
        }

        instance.addRoute("POST", s, route);
        logger.info("Added route: POST " + s);

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }

    /**
     * Add a PUT route
     * @param s the route path
     * @param route the route object
     */
    public static void put(String s, Route route) {
        if (instance == null) {
            instance = new Server();
        }

        instance.addRoute("PUT", s, route);
        logger.info("Added route: PUT " + s);

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }

    /**
     * Set the port of the server. If this method is not called, the default port is 8080
     * @param port the port number
     */
    public static void port(int port) {
        if (instance == null) {
            instance = new Server();
        }

        instance.port = port;
    }

    /**
     * Set the secure port of the server. If this method is not called, the default secure port is 8443
     * @param port the port number
     */
    public static void securePort(int port) {
        if (instance == null) {
            instance = new Server();
        }

        instance.securePort = port;
    }

    /**
     * Set the target host. If this method is not called, routes are added to the default route list
     * @param host the host name
     */
    public static void host(String host) {
        if (instance == null) {
            instance = new Server();
        }

        instance.currentHost = host;
        if (!instance.hostRoutes.containsKey(host)) {
            instance.hostRoutes.put(host, new ArrayList<>());
        } else {
            logger.info("Host already exists: " + host);
        }

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }

    /**
     * Set the location of the static files and start the server
     */
    public static class staticFiles {
        public static void location(String s) {
            if (instance == null) {
                instance = new Server();
            }

            instance.root = s;

            if (!isRunning) {
                isRunning = true;
                // create a new thread to start the server
                new Thread(() -> instance.run()).start();
            }
        }
    }

    /**
     * Add a before route filter
     * @param filter the filter object
     */
    public static void before(RouteFilter filter) {
        if (instance == null) {
            instance = new Server();
        }

        instance.beforeFilters.add(filter);

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }

    /**
     * Add an after route filter
     * @param filter the filter object
     */
    public static void after(RouteFilter filter) {
        if (instance == null) {
            instance = new Server();
        }

        instance.afterFilters.add(filter);

        if (!isRunning) {
            isRunning = true;
            // create a new thread to start the server
            new Thread(() -> instance.run()).start();
        }
    }
}
