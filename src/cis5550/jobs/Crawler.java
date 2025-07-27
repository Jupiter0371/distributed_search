package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Crawler {

    private static final Logger logger = Logger.getLogger(Crawler.class);

    private static final int CONNECTION_TIMEOUT = 10000; // 10 seconds
    private static final int READ_TIMEOUT = 5000; // 5 seconds
    private static final int MAX_FILE_SIZE = 10 * 1024 * 1024; // 10 MB
    private static final int MAX_QUEUE_SIZE = 600000;
    private static final int NUM_URLS_FOR_PARALLEL_GROUPING = 1000;
    private static final int NUM_HOSTS_FOR_PARALLEL_PROCESSING = 16;

    private static final String userAgent = "cis5550-crawler";

    static class Rule {
        String type; // "allow" or "disallow"
        String path;

        Rule(String type, String path) {
            this.type = type;
            this.path = path;
        }
    }

    // List to hold blacklist patterns
    private static final List<String> blacklistPatterns = new ArrayList<>();

    public static void run(FlameContext context, String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            context.output("Usage: Crawler <seed-url(s)> [blacklist-table]");
            return;
        }

        // Check for blacklist table
        if (args.length == 2) {
            String blacklistTableName = args[1];
            loadBlacklistPatterns(context.getKVS(), blacklistTableName);
        }

        // Add seed URLs to the queue
        String seedUrlsArg = args[0];
        if (seedUrlsArg == null || seedUrlsArg.isEmpty()) {
            context.output("Seed URL file not provided");
            return;
        }

        List<String> seedUrls = new ArrayList<>();

        if (!new File(seedUrlsArg).canRead()) {
            // Add the seed URL directly
            seedUrls.add(seedUrlsArg);
            context.output("Crawling Seed URL: " + seedUrlsArg + "\n");
        } else {
            // Read seed URLs from the file
            try (BufferedReader br = new BufferedReader(new FileReader(seedUrlsArg))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        String normalizedUrl = normalizeURL(line.trim(), null);
                        if (normalizedUrl != null && !isUrlBlacklisted(normalizedUrl)) {
                            seedUrls.add(normalizedUrl);
                            context.output("Crawling Seed URL: " + normalizedUrl + "\n");
                        } else if (normalizedUrl == null) {
                            context.output("Invalid seed URL in file: " + line + "\n");
                        } else {
                            context.output("Seed URL is blacklisted: " + line + "\n");
                        }
                    }
                }
            } catch (IOException e) {
                context.output("Error reading seed URL file: " + e.getMessage());
                return;
            }
        }

        // Create a queue with the seed URL
        FlameRDD urlQueue = context.parallelize(seedUrls);

        // Crawl the URLs in the queue
        while (urlQueue.count() > 0 && urlQueue.count() < MAX_QUEUE_SIZE) {
            urlQueue = urlQueue.mapPartitions(iterator -> {
                List<String> urls = new ArrayList<>();

                iterator.forEachRemaining(url_raw -> {
                    String url = normalizeURL(url_raw, null);
                    if (url == null) {
                        logger.error("Failed to normalize URL: " + url_raw);
                    } else {
                        urls.add(url);
                    }
                });

                List<String> newUrls = processUrlsInBatch(urls, context);

                return newUrls.iterator();
            });

            // Sleep for 100ms before crawling the next URL
            // noinspection BusyWait
            Thread.sleep(100);
        }

        context.output("Crawling complete");
    }

    // ----------------------- BATCH PROCESS --------------------------- //

    private static List<String> processUrlsInBatch(List<String> urls, FlameContext context) {
        // Group URLs by host
        Map<String, List<String>> urlsByHost = groupUrlsByHost(urls);

        if (urlsByHost.size() > NUM_HOSTS_FOR_PARALLEL_PROCESSING) {
            logger.info("Using parallel processing for hosts");

            // Process URLs for each host in parallel
            return urlsByHost.entrySet().parallelStream()
                    .flatMap(entry -> {
                        String host = entry.getKey();
                        List<String> hostUrls = entry.getValue();
                        // Process URLs for the host and return a stream of results
                        return processUrlsForHost(host, hostUrls, context).stream();
                    }).collect(Collectors.toList());
        } else {
            logger.info("Using sequential processing for hosts");

            // Process URLs for each host sequentially
            List<String> allNewUrls = new ArrayList<>();
            for (String host : urlsByHost.keySet()) {
                List<String> hostUrls = urlsByHost.get(host);
                allNewUrls.addAll(processUrlsForHost(host, hostUrls, context));
            }
            return allNewUrls;
        }
    }

    private static Map<String, List<String>> groupUrlsByHost(List<String> urls) {
        Map<String, List<String>> urlsByHost;
        if (urls.size() > NUM_URLS_FOR_PARALLEL_GROUPING) {
            logger.info("Using parallel grouping for URLs");

            urlsByHost = new ConcurrentHashMap<>();

            // Use parallel stream to process URLs in parallel
            urls.parallelStream().forEach(url -> {
                try {
                    URL urlObj = new URI(url).toURL();
                    String host = urlObj.getHost();

                    // Use computeIfAbsent to handle thread-safe insertion
                    urlsByHost.computeIfAbsent(host, k -> Collections.synchronizedList(new ArrayList<>()))
                            .add(url);
                } catch (URISyntaxException | MalformedURLException e) {
                    logger.error("Failed to parse URL: " + url + ", error: " + e.getMessage());
                }
            });

        } else {
            logger.info("Using sequential grouping for URLs");

            urlsByHost = new HashMap<>();

            for (String url : urls) {
                try {
                    URL urlObj = new URI(url).toURL();
                    String host = urlObj.getHost();
                    if (!urlsByHost.containsKey(host)) {
                        urlsByHost.put(host, new ArrayList<>());
                    }
                    urlsByHost.get(host).add(url);
                } catch (URISyntaxException | MalformedURLException e) {
                    logger.error("Failed to parse URL: " + url + ", error: " + e.getMessage());
                }
            }

        }
        return urlsByHost;
    }

    private static List<String> processUrlsForHost(String host, List<String> hostUrls, FlameContext context) {
        List<String> newUrls = new ArrayList<>();
        KVSClient kvs = context.getKVS();

        try {
            String hostKey = Hasher.hash(host);

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .connectTimeout(Duration.ofMillis(CONNECTION_TIMEOUT))
                    .build();

            // Fetch and parse robots.txt for the host
            String robotsTxtContent = getRobotsTxt(kvs, hostKey, host, httpClient);
            Map<String, Object> robotsData = parseRobotsTxt(robotsTxtContent);
            double crawlDelay = (double) robotsData.getOrDefault("crawlDelay", 1.0);
            @SuppressWarnings("unchecked")
            List<Rule> rulesToApply = (List<Rule>) robotsData.get("rules");

            // Check rate limiting for the host
            List<String> rateLimitedUrls = checkRateLimiting(kvs, hostKey, hostUrls, crawlDelay);
            if (!rateLimitedUrls.isEmpty()) {
                // Re-add rate-limited URLs to the queue
                newUrls.addAll(rateLimitedUrls);
                return newUrls;
            }

            // Process each URL for the host
            for (String url : hostUrls) {
                try {
                    // Check if the URL has already been crawled
                    String urlKey = Hasher.hash(url);
                    if (kvs.getRow("pt-crawl", urlKey) != null) {
                        logger.info("URL already crawled: " + url);
                        continue;
                    }

                    // Check if the URL is allowed by robots.txt rules
                    URL urlObj = new URI(url).toURL();
                    if (!checkUrlAllowed(urlObj.getPath(), rulesToApply)) {
                        logger.info("URL disallowed by robots.txt: " + url);
                        continue;
                    }

                    // Process the URL and add new URLs discovered
                    newUrls.addAll(processUrl(url, kvs, urlKey, hostKey, httpClient));
                } catch (MalformedURLException e) {
                    logger.error("Malformed URL: " + url + ", error: " + e.getMessage());
                } catch (Exception e) {
                    logger.error("Error processing URL: " + url + ", error: " + e.getMessage());
                }
            }

            // Update rate limiting after processing all URLs for the host
            updateRateLimiting(kvs, hostKey);
        } catch (Exception e) {
            logger.error("Error processing host: " + host + ", error: " + e.getMessage());
        }

        return newUrls;
    }

    // ----------------------- URL PROCESSING --------------------------- //

    private static List<String> processUrl(String url,
                                           KVSClient kvs,
                                           String urlKey,
                                           String hostKey,
                                           HttpClient httpClient) throws IOException {
        List<String> urls = new ArrayList<>();

        // Create a HEAD request
        HttpRequest headRequest = HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", "cis5550-crawler")
                .timeout(Duration.ofMillis(READ_TIMEOUT))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> headResponse;
        try {
            // Send the HEAD request
            headResponse = httpClient.send(headRequest, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            logger.error("Failed to fetch URL (HEAD): " + url + ", error: " + e.getMessage());
            return urls;
        }

        // Create a row for the KVS (Guaranteed to be the first time)
        Row row = new Row(urlKey);
        row.put("url", url);

        // Extract response details
        int headResponseCode = headResponse.statusCode();
        String contentType = headResponse.headers().firstValue("Content-Type").orElse(null);
        int contentLength = (int) headResponse.headers().firstValueAsLong("Content-Length").orElse(-1);

        if (contentType != null) {
            row.put("contentType", contentType);
        }
        if (contentLength != -1) {
            row.put("length", Integer.toString(contentLength));
        }

        // Check if the response code is not 200
        if (headResponseCode != 200) {
            logger.info("During HEAD Request: " + url + " (HTTP status code is not 200: " + headResponseCode + ")");

            // Add info to the table
            row.put("responseCode", Integer.toString(headResponseCode));
            kvs.putRow("pt-crawl", row);

            // Handle redirects
            urls = handleRedirect(headResponseCode, headResponse, url);

            // Update rate limiting
            updateRateLimiting(kvs, hostKey);

            return urls;
        }

        // Update rate limiting
        updateRateLimiting(kvs, hostKey);

        // Check content length
        if (contentLength > MAX_FILE_SIZE) {
            logger.info("Content length exceeds maximum file size: " + url + " (" + contentLength + " bytes)");
            return urls;
        }

        // Check content type
        if (contentType != null && !contentType.toLowerCase().contains("text/html")) {
            logger.info("Content type is not text/html: " + url + " (" + contentType + ")");
            return urls;
        }

        // Open a GET connection to the URL
        urls = getRequest(url, kvs, hostKey, row, contentType, httpClient);
        return urls;
    }

    private static List<String> getRequest(String url,
                                           KVSClient kvs,
                                           String hostKey,
                                           Row row,
                                           String contentType,
                                           HttpClient httpClient) throws IOException {
        List<String> urls = new ArrayList<>();

        // Create a GET request
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", userAgent)
                .timeout(Duration.ofMillis(READ_TIMEOUT))
                .GET()
                .build();

        HttpResponse<byte[]> response;
        try {
            // Send the GET request
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            logger.error("Failed to connect to URL (GET request): " + url + ", error: " + e.getMessage());
            return urls;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Request to URL was interrupted: " + url);
            return urls;
        }

        int responseCode = response.statusCode();
        byte[] content = response.body();

        row.put("responseCode", Integer.toString(responseCode));
        if (responseCode != 200) {
            // Log error for non-200 response codes
            logger.error("Failed to fetch URL: " + url + " (HTTP status code: " + responseCode + ")");

            kvs.putRow("pt-crawl", row);

            // Update rate limiting
            updateRateLimiting(kvs, hostKey);

            return urls;
        }

        // Normalize the content
        content = contentNormalize(content);

        // Compute the content hash
        String contentHash = Hasher.hash(new String(content));

        // Check if the content hash exists in 'content-hashes' table
        Row hashRow = kvs.getRow("content-hashes", contentHash);
        if (hashRow != null) {
            // Duplicate content found
            String canonicalUrl = hashRow.get("url");

            // Only set 'canonicalURL' if it's different from the current URL
            if (!canonicalUrl.equals(url)) {
                // Add 'canonicalURL' column to 'pt-crawl' table
                row.put("canonicalURL", canonicalUrl);
                logger.info("Duplicate content found: " + url + " (canonical URL: " + canonicalUrl + ")");
            } else {
                // Add page normally
                if (contentType != null && contentType.toLowerCase().contains("text/html")) {
                    row.put("page", content);
                }
            }

            kvs.putRow("pt-crawl", row);
        } else {
            // Add the content to the row
            if (contentType != null && contentType.toLowerCase().contains("text/html")) {
                row.put("page", content);
            }
            kvs.putRow("pt-crawl", row);

            // Store the content hash in 'content-hashes' table
            Row hashRowNew = new Row(contentHash);
            hashRowNew.put("url", url);
            kvs.putRow("content-hashes", hashRowNew);
        }

        // Update rate limiting
        updateRateLimiting(kvs, hostKey);

        urls = extractURLs(new String(content), url);
        return urls;
    }

    private static List<String> handleRedirect(int responseCode, HttpResponse<?> response, String baseUrl) {
        List<String> urls = new ArrayList<>();

        if (responseCode == 301 || // HTTP_MOVED_PERM
                responseCode == 302 || // HTTP_MOVED_TEMP
                responseCode == 303 || // HTTP_SEE_OTHER
                responseCode == 307 || // Temporary Redirect
                responseCode == 308) { // Permanent Redirect

            // Extract the Location header from the response
            String location = response.headers().firstValue("Location").orElse(null);
            if (location != null) {
                // Normalize the redirect URL and add it to the list
                String normalizedUrl = normalizeURL(location, baseUrl);
                if (normalizedUrl != null) {
                    urls.add(normalizedUrl);
                }
            }
        }

        return urls;
    }

    // ----------------------- ROBOTS TXT --------------------------- //

    private static String getRobotsTxt(KVSClient kvs, String key, String host, HttpClient httpClient) throws IOException, URISyntaxException {
        String robotsTxtContent;

        Row row = kvs.getRow("hosts", key);

        if (row != null && row.get("robotsTxt") != null) {
            robotsTxtContent = row.get("robotsTxt");
        } else {
            // Try fetching robots.txt using HTTPS first
            robotsTxtContent = fetchRobotsTxt(host, "https", httpClient);
            if (robotsTxtContent == null) {
                // Fallback to HTTP if HTTPS fails
                robotsTxtContent = fetchRobotsTxt(host, "http", httpClient);
            }

            if (robotsTxtContent == null) {
                // No robots.txt found
                return null;
            }

            // Store robots.txt content in hosts table
            if (row == null) {
                row = new Row(key);
            }
            row.put("robotsTxt", robotsTxtContent);
            kvs.putRow("hosts", row);
        }

        return robotsTxtContent;
    }

    private static String fetchRobotsTxt(String host, String protocol, HttpClient httpClient) throws IOException, URISyntaxException {
        String robotsTxtUrl = protocol + "://" + host + "/robots.txt";

        // Create a URI for the robots.txt URL
        URI robotsTxtUri = new URI(robotsTxtUrl);

        // Create a GET request for robots.txt
        HttpRequest request = HttpRequest.newBuilder(robotsTxtUri)
                .header("User-Agent", "cis5550-crawler")
                .timeout(Duration.ofMillis(2000)) // 2 seconds
                .GET()
                .build();

        HttpResponse<byte[]> response;
        try {
            // Send the GET request using the provided HttpClient
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            logger.error("Failed to connect to robots.txt URL: " + robotsTxtUrl + ", error: " + e.getMessage());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Request to robots.txt was interrupted: " + robotsTxtUrl);
            return null;
        }

        // Check the response code
        int responseCode = response.statusCode();
        if (responseCode != 200) {
            logger.error("Failed to fetch robots.txt: " + robotsTxtUrl + " (HTTP status code: " + responseCode + ")");
            return null;
        }

        // Read the content of robots.txt
        byte[] responseBody = response.body();
        return new String(responseBody);
    }

    private static Map<String, Object> parseRobotsTxt(String robotsTxtContent) throws IOException {
        Map<String, Object> result = new HashMap<>();
        double crawlDelay = 1.0;
        List<Rule> rulesToApply = new ArrayList<>();

        if (robotsTxtContent == null || robotsTxtContent.isEmpty()) {
            result.put("crawlDelay", crawlDelay);
            result.put("rules", rulesToApply);
            return result;
        }

        // Read robots.txt content by lines
        BufferedReader reader = new BufferedReader(new StringReader(robotsTxtContent));
        String line;
        List<String> currentUserAgents = new ArrayList<>();
        Map<String, List<Rule>> agentsRules = new LinkedHashMap<>();
        Map<String, Double> agentsCrawlDelay = new HashMap<>();

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            if (line.toLowerCase().startsWith("user-agent:")) {
                String agent = line.substring(11).trim().toLowerCase();
                currentUserAgents.clear();
                currentUserAgents.add(agent);
                if (!agentsRules.containsKey(agent)) {
                    agentsRules.put(agent, new ArrayList<>());
                }
            } else if (line.toLowerCase().startsWith("disallow:")) {
                String path = line.substring(9).trim();
                for (String agent : currentUserAgents) {
                    agentsRules.get(agent).add(new Rule("disallow", path));
                }
            } else if (line.toLowerCase().startsWith("allow:")) {
                String path = line.substring(6).trim();
                for (String agent : currentUserAgents) {
                    agentsRules.get(agent).add(new Rule("allow", path));
                }
            } else if (line.toLowerCase().startsWith("crawl-delay:")) {
                String delayValue = line.substring(12).trim();
                try {
                    // Extract the numeric part of the crawl-delay value
                    String numericValue = delayValue.split("\\s+")[0];
                    double cd = Double.parseDouble(numericValue);

                    for (String agent : currentUserAgents) {
                        agentsCrawlDelay.put(agent, cd);
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Invalid Crawl-delay value: " + delayValue);
                }
            }
        }

        // Determine which rules to apply
        String ua = Crawler.userAgent.toLowerCase();
        if (agentsRules.containsKey(ua)) {
            rulesToApply = agentsRules.get(ua);
            if (agentsCrawlDelay.containsKey(ua)) {
                crawlDelay = agentsCrawlDelay.get(ua);
            }
        } else if (agentsRules.containsKey("*")) {
            rulesToApply = agentsRules.get("*");
            if (agentsCrawlDelay.containsKey("*")) {
                crawlDelay = agentsCrawlDelay.get("*");
            }
        }

        result.put("crawlDelay", crawlDelay);
        result.put("rules", rulesToApply);
        return result;
    }

    private static Boolean checkUrlAllowed(String path, List<Rule> rulesToApply) {
        if (rulesToApply == null || rulesToApply.isEmpty()) {
            return true;
        }

        // Apply rules in order; first match counts
        for (Rule rule : rulesToApply) {
            String rulePath = rule.path;
            if (rulePath.isEmpty() || rulePath.equals("/")) {
                rulePath = "/";
            }
            if (path.startsWith(rulePath)) {
                if (rule.type.equals("allow")) {
                    return true; // Allowed
                } else if (rule.type.equals("disallow")) {
                    return false; // Disallowed
                }
            }
        }

        // No matching rule; allow all
        return true;
    }

    // ----------------------- RATE LIMITING --------------------------- //

    private static List<String> checkRateLimiting(KVSClient kvs, String key, List<String> urls, double crawlDelay)
            throws IOException {
        List<String> rateLimitedUrls = new ArrayList<>();

        Row row = kvs.getRow("hosts", key);

        long lastAccessTime = 0;

        if (row != null && row.get("lastAccessTime") != null) {
            lastAccessTime = Long.parseLong(row.get("lastAccessTime"));
        }

        long currentTime = System.currentTimeMillis();
        long delayInMillis = (long) (crawlDelay * 1000);

        if (currentTime - lastAccessTime < delayInMillis) {
            // Add all URLs back to the queue for retrying later
            rateLimitedUrls.addAll(urls);
        }

        return rateLimitedUrls;
    }

    private static void updateRateLimiting(KVSClient kvs, String key) throws IOException {
        Row row = kvs.getRow("hosts", key);
        if (row == null) {
            row = new Row(key);
        }
        row.put("lastAccessTime", Long.toString(System.currentTimeMillis()));
        kvs.putRow("hosts", row);
    }

    // ----------------------- EXTRACT & NORMALIZE --------------------------- //

    protected static List<String> extractURLs(String content, String baseUrl) {
        int chunkSize = 10000; // Divide content into chunks of 10KB
        int overlap = 100; // Number of characters to overlap

        List<String> chunks = IntStream.range(0, (content.length() + chunkSize - 1) / chunkSize)
                .mapToObj(i -> {
                    int start = i * chunkSize;
                    int end = Math.min(start + chunkSize, content.length());

                    // Adjust start to the last '<' before the current start (if not the first chunk)
                    if (i != 0) {
                        int adjustedStart = content.lastIndexOf('<', start);
                        if (adjustedStart != -1 && adjustedStart > (i * chunkSize) - overlap) {
                            start = adjustedStart;
                        }
                    } else {
                        // Handle case where first chunk has text before the first '<'
                        int firstTag = content.indexOf('<', start);
                        if (firstTag != -1 && firstTag < end) {
                            start = firstTag; // Skip leading text before the first tag
                        }
                    }

                    // Adjust end to the next '>' after the current end (if not the last chunk)
                    if (i != (content.length() + chunkSize - 1) / chunkSize - 1) {
                        int adjustedEnd = content.indexOf('>', end);
                        if (adjustedEnd != -1 && adjustedEnd < (i + 1) * chunkSize + overlap) {
                            end = adjustedEnd + 1;
                        }
                    }

                    // Check for overlapping <a> tags at the chunk boundaries
                    String chunk = content.substring(start, end);

                    // If the chunk ends in a partially open <a> tag, extend the chunk to complete the tag
                    int openAnchor = chunk.lastIndexOf("<a");
                    int closeAnchor = chunk.lastIndexOf("</a>");
                    if (openAnchor > closeAnchor) {
                        int nextCloseAnchor = content.indexOf("</a>", end);
                        if (nextCloseAnchor != -1) {
                            end = Math.min(content.length(), nextCloseAnchor + 4); // Include the entire </a>
                            chunk = content.substring(start, end);
                        }
                    }

                    // If the chunk starts with a </a>, skip it
                    if (chunk.startsWith("</a>")) {
                        start = content.indexOf('>', start) + 1; // Move start after </a>
                        chunk = content.substring(start, end);
                    }

                    return chunk;
                })
                .toList();

        // Process chunks in parallel
        return chunks.parallelStream()
                .flatMap(chunk -> processChunk(chunk, baseUrl).stream())
                .collect(Collectors.toList());
    }

    private static List<String> processChunk(String chunk, String baseUrl) {
        List<String> urls = new ArrayList<>();
        int index = 0;
        Pattern hrefPattern = Pattern.compile("\\bhref\\s*=\\s*(?:'([^']*)'|\"([^\"]*)\"|([^\\s>]+))", Pattern.CASE_INSENSITIVE);

        while (index < chunk.length()) {
            // Find the next opening tag '<'
            int tagStart = chunk.indexOf('<', index);
            if (tagStart == -1) {
                break;
            }

            // Find the next closing tag '>'
            int tagEnd = chunk.indexOf('>', tagStart);
            if (tagEnd == -1) {
                break;
            }

            // Extract the tag content
            String tag = chunk.substring(tagStart + 1, tagEnd);

            // Ignore closing tags
            if (tag.startsWith("/")) {
                index = tagEnd + 1;
                continue;
            }

            // Check if the tag is an anchor tag (case-insensitive)
            if (!tag.toLowerCase().startsWith("a ")) {
                index = tagEnd + 1;
                continue;
            }

            // Find the href attribute
            Matcher matcher = hrefPattern.matcher(tag);
            if (matcher.find()) {
                // Single quote / double quote / no quote
                String hrefValue = matcher.group(1);
                if (hrefValue == null)
                    hrefValue = matcher.group(2);
                if (hrefValue == null)
                    hrefValue = matcher.group(3);
                processHref(urls, hrefValue, baseUrl);
            }

            index = tagEnd + 1;
        }

        return urls;
    }

    private static void processHref(List<String> urls, String hrefValue, String baseUrl) {
        if (hrefValue != null && !hrefValue.isEmpty()) {
            // logger.info("Found URL: " + hrefValue);
            String normalizedHref = normalizeURL(hrefValue, baseUrl);
            // logger.info("Normalized URL: " + normalizedHref);
            if (normalizedHref != null) {
                // Check against blacklist patterns
                if (!isUrlBlacklisted(normalizedHref)) {
                    urls.add(normalizedHref);
                } else {
                    logger.info("URL blacklisted: " + normalizedHref);
                }
            }
        }
    }

    private static String normalizeURL(String href, String baseUrl) {
        try {
            // Remove fragment identifier
            int hashIndex = href.indexOf('#');
            if (hashIndex != -1) {
                href = href.substring(0, hashIndex);
            }

            // Clean up the href
            href = cleanHref(href);
            if (href.isEmpty()) {
                // The base URL should be guaranteed to be normalized
                return baseUrl;
            }

            // Preprocess the href to handle spaces and illegal characters
            href = href.replace("\\", "/"); // Replace backslashes with slashes
            href = href.replace("\"", ""); // Remove double quotes

            URI baseUri = baseUrl != null ? new URI(baseUrl) : null;
            URI hrefUri = new URI(href);

            URI resolvedUri = baseUri != null ? baseUri.resolve(hrefUri) : hrefUri;

            // Normalize the URI to resolve '..' and '.'
            resolvedUri = resolvedUri.normalize();

            // Only consider http and https protocols
            String scheme = resolvedUri.getScheme();
            if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
                return null;
            }

            // Ignore URLs that end with large media files
            String path = resolvedUri.getPath();
            if (path != null && path.matches(
                    ".*\\.(jpg|jpeg|gif|png|txt|pdf|bmp|ico|tiff|svg|webp|heic|mp4|mkv|avi|mov|mp3|zip|rar)$")) {
                return null;
            }

            // Get the normalized URL without re-encoding
            return getNormalizedUrl(resolvedUri);
        } catch (Exception e) {
            logger.error("Failed to normalize URL: " + href + ", error: " + e.getMessage());
            return null;
        }
    }

    private static String getNormalizedUrl(URI uri) {
        // Always include the port, even if it's the default
        String scheme = uri.getScheme().toLowerCase();
        String host = uri.getHost() != null ? uri.getHost().toLowerCase() : null;
        int port = uri.getPort();

        if (port == -1) {
            if (scheme.equals("http")) {
                port = 80;
            } else if (scheme.equals("https")) {
                port = 443;
            }
        }

        // Build the normalized URL manually to avoid re-encoding
        StringBuilder sb = new StringBuilder();

        // Scheme
        sb.append(scheme).append("://");

        // User Info (if any)
        if (uri.getUserInfo() != null) {
            sb.append(uri.getUserInfo()).append("@");
        }

        // Host
        sb.append(host);

        // Port
        sb.append(":").append(port);

        // Path
        String rawPath = uri.getRawPath();
        if (rawPath != null && !rawPath.isEmpty()) {
            sb.append(rawPath);
        } else {
            sb.append("/");
        }

        // Query
        String rawQuery = uri.getRawQuery();
        if (rawQuery != null) {
            sb.append("?").append(rawQuery);
        }

        return sb.toString();
    }

    private static String cleanHref(String href) {
        // Remove control characters except for whitespace
        href = href.replaceAll("\\p{Cntrl}&&[^\n" +
                "\t]", "");
        // Remove line breaks and tabs
        href = href.replaceAll("[\\r\\n\\t]", "");
        // Remove leading/trailing whitespace
        href = href.trim();
        return href;
    }

    // ----------------------- BLACKLIST --------------------------- //

    private static void loadBlacklistPatterns(KVSClient kvs, String blacklistTableName) throws IOException {
        // Fetch all rows from the blacklist table
        Iterator<Row> iterator = kvs.scan(blacklistTableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String pattern = row.get("pattern");
            if (pattern != null && !pattern.isEmpty()) {
                blacklistPatterns.add(pattern);
            }
        }
        logger.info("Loaded " + blacklistPatterns.size() + " blacklist patterns.");
    }

    private static boolean isUrlBlacklisted(String url) {
        for (String pattern : blacklistPatterns) {
            if (wildcardMatch(pattern, url)) {
                return true;
            }
        }
        return false;
    }

    private static boolean wildcardMatch(String pattern, String url) {
        String regex = wildcardToRegex(pattern);
        // Add case-insensitive flag
        regex = "(?i)" + regex;
        return url.matches(regex);
    }

    private static String wildcardToRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '*') {
                sb.append(".*");
            } else if ("\\.[]{}()+-^$|?".indexOf(c) != -1) {
                sb.append("\\").append(c); // Escape regex special characters
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ----------------------- CONTENT NORMALIZATION --------------------------- //

    private static byte[] contentNormalize(byte[] content) {
        String contentStr = new String(content);

        // Remove any images and svgs
        contentStr = contentStr.replaceAll("<img[^>]*>", "");
        contentStr = contentStr.replaceAll("<svg[^>]*>", "");

        // Remove any scripts
        contentStr = contentStr.replaceAll("<script[^>]*>[\\s\\S]*?</script>", "");

        // Remove any videos
        contentStr = contentStr.replaceAll("<video[^>]*>[\\s\\S]*?</video>", "");

        // Remove any styles
        contentStr = contentStr.replaceAll("<style[^>]*>[\\s\\S]*?</style>", "");

        // Remove any comments
        contentStr = contentStr.replaceAll("<!--[^>]*-->", "");

        // Remove any links
        contentStr = contentStr.replaceAll("<link[^>]*>", "");

        // Remove any meta tags
        contentStr = contentStr.replaceAll("<meta[^>]*>", "");

        return contentStr.getBytes();
    }
}