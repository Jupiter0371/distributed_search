package cis5550.backend;

import cis5550.jobs.Ranker;
import cis5550.kvs.KVSClient;
import cis5550.tools.Logger;
import cis5550.tools.SpellChecker;
import cis5550.webserver.Server.staticFiles;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import static cis5550.jobs.Ranker.processQuery;
import static cis5550.webserver.Server.*;

public class Backend {

    private static final Logger logger = Logger.getLogger(Backend.class);

    private static int port = 8080;
    private static int securePort = 8443;
    private static String root = "public";
    private static final List<String> queryHistory = new ArrayList<>();
    private static final int MAX_HISTORY_SIZE = 10;

    public static void main(String[] args) throws Exception {
        if (args.length < 3 && args.length > 4) {
            System.err.println("Usage: Backend <kvs-hostname:port> [-r root] [-http port] [-https securePort]");
            System.exit(1);
        }

        // Get the KVS address
        String kvsHostname = args[0];

        // Parse optional arguments
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-r":
                    if (i + 1 < args.length) {
                        root = args[++i];
                    } else {
                        System.err.println("Missing value for -r option");
                        System.exit(1);
                    }
                    break;
                case "-http":
                    if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid HTTP port number: " + args[i]);
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Missing value for -http option");
                        System.exit(1);
                    }
                    break;
                case "-https":
                    if (i + 1 < args.length) {
                        try {
                            securePort = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid HTTPS port number: " + args[i]);
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Missing value for -https option");
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
            }
        }

        logger.info("Starting backend server on HTTP port " + port + " and HTTPS port " + securePort);

        // Start the server
        port(port);
        securePort(securePort);
        staticFiles.location(root);
        KVSClient kvs = new KVSClient(kvsHostname);
        SpellChecker spellChecker = new SpellChecker();
        spellChecker.loadDictionary("words.txt");

        // Provide a home page
        get("/", (req, res) -> {
            res.redirect("/index.html", 301);
            return null;
        });

        // Get the query history
        get("/history", (req, res) -> {
            String query = req.queryParams("q");

            if (query != null && !query.trim().isEmpty()) {
                queryHistory.add(query);
                if (queryHistory.size() > MAX_HISTORY_SIZE) {
                    queryHistory.remove(0);
                }
            }

            logger.info("Query history: " + queryHistory);

            StringBuilder html = new StringBuilder();
            html.append("<ul id=\"searchHistoryList\" class=\"list-group\">");
            for (String q : queryHistory) {
                html.append("<li class=\"list-group-item\">")
                    .append("<a href=\"/search.html?q=").append(q).append("\">")
                    .append(q).append("</a></li>");
            }
            html.append("</ul>");
            return html.toString();

        });
      
        get("/search", (req, res) -> {
            String query = req.queryParams("q");
            if (query == null || query.trim().isEmpty()) {
                res.status(400, "Bad Request");
                return "<li class=\"list-group-item\">No query provided. Please try again.</li>";
            }

            // Validate query to ensure it contains only normal English characters
            if (!query.matches("[a-zA-Z0-9 .,!?\"'()-]*")) { // Adjust regex as needed
                res.status(400, "Bad Request");
                return "<li class=\"list-group-item\">Query contains invalid characters. " +
                        "Please use only English letters, numbers, and basic punctuation.</li>";
            }
            
            int pageNum;
            try {
            	pageNum = Integer.parseInt(req.queryParams("pageNum"));
            } catch (NumberFormatException e) {
                // Handle invalid or missing "pgNum" parameter
                res.status(400, "Bad Request"); // Set HTTP status to Bad Request
                //res.body("Invalid or missing 'pgNum' parameter. Please provide a valid integer.");
                return "<li class=\"list-group-item\">Invalid or missing 'pageNum' parameter. Please provide a valid integer.</li>";
            };
           
            if(pageNum <= 0) {
            	res.status(400, "Bad Request");
                return "<li class=\"list-group-item\">Page Number need to be greater than or equal to one. Please try again.</li>";
            }
            
            int pageSize;
            try {
                pageSize = Integer.parseInt(req.queryParams("pageSize"));
            } catch (NumberFormatException e) {
                // Handle invalid or missing "pgNum" parameter
                res.status(400, "Bad Request"); // Set HTTP status to Bad Request
                //res.body("Invalid or missing 'pgNum' parameter. Please provide a valid integer.");
                return "<li class=\"list-group-item\">Invalid or missing 'pageSize' parameter. Please provide a valid integer.</li>";
            };
                      
            if (pageSize <= 0) {
            	res.status(400, "Bad Request");
                return "<li class=\"list-group-item\">Page Size need to be greater than or equal to one. Please try again.</li>";
            }

            // Process the query and get results
            Ranker.SearchList results = processQuery(kvs, query, pageNum, pageSize, spellChecker);

            // Total number of results
            int totalResults = results.totalResults();
            int totalPages = (int) Math.ceil((double) totalResults / pageSize);

            // Build results HTML
            StringBuilder html = new StringBuilder();
            if (results.searchResults().isEmpty()) {
                html.append("<div class=\"card mb-3\">")
                        .append("<div class=\"card-body\">No results found.</div>")
                        .append("</div>");
            } else {
                for (Ranker.SearchResult result : results.searchResults()) {
                    try {
                        // Fetch title and snippet
                        String pageContent = fetchPageContent(result.url());
                        String title = extractTitle(pageContent);
                        String snippet = extractPreview(pageContent);

                        // Ensure dynamic content is escaped
                        title = title != null ? escapeHtml(title) : "Untitled Page";
                        snippet = snippet != null ? escapeHtml(snippet) : "";

                        html.append("<div class=\"card mb-3\">")
                                .append("<div class=\"card-body\">")
                                .append("<h5 class=\"card-title\"><a href=\"").append(result.url()).append("\" target=\"_blank\">")
                                .append(title).append("</a></h5>")
                                .append("<h6 class=\"card-subtitle mb-2 text-muted\">").append(result.url()).append("</h6>")
                                .append("<p class=\"card-text\">").append(snippet).append("</p>")
                                .append("</div>")
                                .append("</div>");
                    } catch (Exception e) {
                        logger.error("Error processing URL: " + result.url(), e);
                        html.append("<div class=\"card mb-3 text-danger\">")
                                .append("<div class=\"card-body\">")
                                .append("<p class=\"card-text\">Error processing result: ").append(result.url()).append("</p>")
                                .append("</div>")
                                .append("</div>");
                    }
                }
            }

            // Include pagination metadata
            html.append("<script>");
            html.append("window.paginationData = { currentPage: ").append(pageNum)
                    .append(", totalPages: ").append(totalPages).append(" };");
            html.append("</script>");

            return html.toString();
        });
    }

    private static String fetchPageContent(String urlStr) throws Exception {
        URL url = new URI(urlStr).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
            return content.toString();
        } catch (Exception e) {
            logger.error("Error fetching page content: " + urlStr, e);
            return "<html><head><title>Error</title></head><body><h1>Error fetching page content</h1></body></html>";
        }
    }

    private static String extractTitle(String html) {
        int titleStart = html.indexOf("<title>");
        int titleEnd = html.indexOf("</title>");
        if (titleStart != -1 && titleEnd != -1 && titleEnd > titleStart) {
            return html.substring(titleStart + 7, titleEnd).trim();
        }
        return null;
    }

    private static String extractPreview(String htmlContent) {
        // Extract content from specific tags (e.g., <p>, <h1>, <h2>)
        Pattern pattern = Pattern.compile("<(p|h1|h2)>(.*?)</\\1>", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(htmlContent);

        StringBuilder previewBuilder = new StringBuilder();

        while (matcher.find() && previewBuilder.length() < 200) {
            previewBuilder.append(matcher.group(2).replaceAll("<[^>]*>", " ").trim()).append(" ");
        }

        String preview = previewBuilder.toString().trim();
        if (preview.isEmpty()) {
            // Fallback to plain text extraction if no match is found
            return extractPreviewFromPlainText(htmlContent);
        }

        // Truncate and normalize whitespace
        preview = preview.replaceAll("\\s+", " ");
        return preview.length() > 200 ? preview.substring(0, 200) + "..." : preview;
    }

    private static String extractPreviewFromPlainText(String htmlContent) {
        // Remove all HTML tags and normalize whitespace
        String plainText = htmlContent.replaceAll("<[^>]*>", " ").trim().replaceAll("\\s+", " ");
        return plainText.length() > 200 ? plainText.substring(0, 200) + "..." : plainText;
    }

    private static String escapeHtml(String input) {
        return input.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#x27;");
    }
}
