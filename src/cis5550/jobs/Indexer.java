package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Indexer {

    private static final Logger logger = Logger.getLogger(Indexer.class);

    private static final String delimiter = "\u0001";
    private static final String delimiter2 = "\u0002";
    private static final String delimiter3 = "\u0003";
    private static final String delimiter4 = "\u0004";

    private static final Set<String> stopWords = new HashSet<>(Arrays.asList(
            "a", "an", "the", "and", "or", "but", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "shall", "should", "may", "might",
            "must", "can", "could", "in", "on", "at", "of", "to", "for", "with", "about", "against",
            "between", "into", "through", "during", "before", "after", "above", "below", "from", "up",
            "down", "out", "over", "under", "again", "further", "then", "once", "here", "there", "when",
            "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some",
            "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t",
            "can", "will", "just", "don", "should", "now"
    ));

    private static final double a = 0.5;
    private static final int MAX_WORD_LENGTH = 120; // To ensure that it can be stored on the disk

    public static void run(FlameContext context, String[] args) throws Exception {
        logger.info("Starting Indexer job...\n");

        // Load the 'pt-crawl' table
        FlameRDD rdd = context.fromTable("pt-crawl", row -> {
            if (row.get("url") != null && row.get("page") != null) {
                // Get the page content
                String pageContent = new String(row.getBytes("page"), StandardCharsets.UTF_8).replace(delimiter, " ");
                // Get the URL
                String url = row.get("url").replace(delimiter, "");
                // Store the URL and content in the KVS
                return url + delimiter + pageContent;
            } else {
                return null;
            }
        });

        // Count the number of documents
        long N = rdd.count();

        // Convert RDD to PairRDD of (url, pageContent)
        FlamePairRDD urlPageContent = rdd.flatMapToPair(s -> {
            String[] parts = s.split(delimiter, 2);
            String url = parts[0];
            String pageContent = parts[1];

            // Hash the URL
            String hashedUrl = Hasher.hash(url);
            // Combine the URL and the pagecontent
            String combined = url + delimiter + pageContent;

            if (parts.length == 2) {
                return Collections.singletonList(new FlamePair(hashedUrl, combined));
            } else {
                logger.warn("Skipping invalid record: " + s);
                return Collections.emptyList();
            }
        });
        context.getKVS().delete(rdd.getTableName());

        // Convert PairRDD (url, pageContent) to PairRDD of (word/phrase, url:positions:count:tf)
        FlamePairRDD wordPhraseUrls = urlPageContent.flatMapToPair(pair -> {
            String combined = pair._2();
            String[] parts = combined.split(delimiter, 2);

            String originalUrl = parts[0];
            String decodedUrl = URLDecoder.decode(originalUrl, StandardCharsets.UTF_8); // Use a separate variable for the decoded URL

            String pageContent = parts[1];
            // Delete non-ASCII characters
            pageContent = pageContent.replaceAll("[^\\x00-\\x7F]", "");

            // Extract anchor texts & header & keyword from url
            String anchorTexts = extractAnchorTexts(pageContent);
            String pageContentWithAnchorTexts = pageContent + " " + anchorTexts;
            Set<String> urlKeywords = preprocessAndStem(extractKeywordsFromURL(decodedUrl));
            Set<String> criticalPageContent = preprocessAndStem(extractCriticalPageElements(pageContent));

            // Split the page content into words
            String[] wordsArray = getWordsArray(pageContentWithAnchorTexts);

            // Map to keep track of word positions
            Map<String, List<Integer>> wordPositionsMap = new HashMap<>();

            // Map to keep track of word frequency
            Map<String, Integer> wordFrequencyMap = new HashMap<>();

            PorterStemmer stemmer = new PorterStemmer();

            // Initialize an adjusted index counter
            int adjustedIndex = 0;

            // Process words and phrases
            for (String s : wordsArray) {
                String word = s.toLowerCase();
                // Filter words: exclude long words, malformed words, stop words, and empty words
                if (word.length() > MAX_WORD_LENGTH || word.matches("^[#@&|/_~=^*$%]+$") || word.isEmpty() || stopWords.contains(word)) {
                    continue; // Skip this word
                }

                // Stem the word
                for (char ch : word.toCharArray()) {
                    stemmer.add(ch);
                }

                stemmer.stem();
                String stemmedWord = stemmer.toString(); // Get the stemmed version of the word

                // Store adjusted positions
                wordPositionsMap.computeIfAbsent(stemmedWord, k -> new ArrayList<>()).add(adjustedIndex + 1); // Use adjusted index
                wordFrequencyMap.put(stemmedWord, wordFrequencyMap.getOrDefault(stemmedWord, 0) + 1); // Count frequency

                adjustedIndex++; // Increment adjusted index only for valid words

            }

            // Find the maximum frequency for words
            int maxWordFrequency = wordFrequencyMap.values().stream().max(Integer::compare).orElse(1);
            List<FlamePair> wordPairs = wordPositionsMap.entrySet().stream()
                    .map(entry -> createFlamePair(entry, wordFrequencyMap,
                            maxWordFrequency, decodedUrl, criticalPageContent, urlKeywords, true))
                    .toList();

            List<FlamePair> combinedPairs = new ArrayList<>(wordPairs);

            if (combinedPairs.isEmpty()) {
                logger.warn("No words or phrases found in: " + decodedUrl);
                return Collections.emptyList();
            }

            return combinedPairs;
        });
        context.getKVS().delete(urlPageContent.getTableName());

        // Aggregate word and phrase entries (word/phrase, N:url:positions:count:tf)
        FlamePairRDD invertedIndex = wordPhraseUrls.foldByKey("", (accum, value) -> {
            // Build for the single new value
            //logger.info("value"+value);
            String newVal = processNewUrlString(value);
            //logger.info("new Val"+newVal);
            return newVal.isEmpty() ? accum : accum.isEmpty() ? N + delimiter4 + newVal : accum + delimiter + newVal;
        });
        context.getKVS().delete(wordPhraseUrls.getTableName());

        // Save the inverted index as 'pt-index' table
        invertedIndex.saveAsTable("pt-index");

        context.output("Indexing complete.");
    }

    private static FlamePair createFlamePair(Map.Entry<String, List<Integer>> entry, Map<String, Integer> frequencyMap,
                                             int maxFrequency, String url, Set<String> criticalPageContent,
                                             Set<String> urlKeywords, boolean isWord) {
        String key = entry.getKey();
        List<Integer> positions = entry.getValue();
        int frequency = frequencyMap.get(key);
        double weight = isWord && isImportantWord(key, criticalPageContent, urlKeywords) ? 0.5 : 0;
        double tf = a + (1 - a) * frequency / maxFrequency;
        String weightedTf = String.valueOf(tf + weight);

        String value = getValue(positions, url, weightedTf);
        return new FlamePair(key, value);
    }

    private static String[] getWordsArray(String pageContent) {
        // Remove script and style contents
        pageContent = pageContent.replaceAll("(?s)<script.*?>.*?</script>", " ");
        pageContent = pageContent.replaceAll("(?s)<style.*?>.*?</style>", " ");

        // Remove all other HTML tags
        String textOnly = pageContent.replaceAll("<[^>]*>", " ");

        // Decode common HTML entities (e.g., &amp;, &lt;, &gt;)
        textOnly = textOnly.replaceAll("&amp;", "&")
                .replaceAll("&lt;", "<")
                .replaceAll("&gt;", ">")
                .replaceAll("&quot;", "\"")
                .replaceAll("&apos;", "'")
                .replaceAll("&[a-zA-Z0-9#]+;", " ");

        // Remove punctuation except for hyphens and apostrophes
        textOnly = textOnly.replaceAll("[.,:;!?'\"()\\[\\]{}<>]", " ");

        // Remove isolated hyphens (not part of hyphenated words)
        textOnly = textOnly.replaceAll("\\b-\\b", " ");

        // Remove leading/trailing apostrophes
        textOnly = textOnly.replaceAll("(?<=\\b)'|'(?=\\b)", "");

        // Remove control characters
        textOnly = textOnly.replaceAll("[\\r\\n\\t]", " ");

        // Normalize whitespace
        textOnly = textOnly.replaceAll("\\s+", " ").trim();

        return textOnly.split("\\s+");
    }

    private static String getValue(List<Integer> positions, String url, String tf) {
        StringBuilder positionsList = new StringBuilder();
        for (int position : positions) {
            if (!positionsList.isEmpty()) {
                positionsList.append(" ");
            }
            positionsList.append(position);
        }

        // Get the word count
        int count = positions.size();

        return url + delimiter2 + positionsList.toString() + delimiter2 + count + delimiter2 + tf;
    }

    private static String extractAnchorTexts(String pageContent) {
        StringBuilder anchorTexts = new StringBuilder();
        String pageContentLower = pageContent.toLowerCase();
        int start = 0;

        while (start != -1) {
            // Find the opening <a> tag
            start = pageContentLower.indexOf("<a", start);
            if (start != -1) {
                // Find the closing '>' of the opening <a> tag
                int tagClose = pageContentLower.indexOf(">", start);
                if (tagClose != -1) {
                    // Find the closing </a> tag
                    int end = pageContentLower.indexOf("</a>", tagClose);
                    if (end != -1) {
                        // Validate substring indices
                        if (tagClose + 1 < pageContent.length() && end <= pageContent.length()) {
                            String anchorText = pageContent.substring(tagClose + 1, end).trim();

                            // Add non-empty anchor text
                            if (!anchorText.isEmpty()) {
                                anchorTexts.append(anchorText).append(" ");
                            }
                        }

                        // Update start to continue searching
                        start = end + 4;
                    } else {
                        // No closing </a> found, exit loop
                        break;
                    }
                } else {
                    // Malformed <a> tag, exit loop
                    break;
                }
            }
        }

        return anchorTexts.toString().trim();
    }

    private static String processNewUrlString(String entry) {
        // url:positions:count:tf
        String[] parts = entry.split(delimiter2, 4);
        if (parts.length != 4) {
            logger.warn("Invalid entry format: " + entry);
            return "";
        }

        String url;
        String positions;
        try {
            url = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            positions = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.warn("Error decoding URL or positions: " + entry);
            return "";
        }

        int count;
        try {
            count = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            logger.warn("Invalid count in entry: " + entry);
            return "";
        }
        double tf;
        try {
            tf = Double.parseDouble(parts[3]);
        } catch (NumberFormatException e) {
            logger.warn("Invalid tf in entry: " + entry);
            return "";
        }

        // Format as url:positions:tf
        return url + delimiter3 + positions + delimiter3 + count + delimiter3 + tf;
    }

    private static String extractKeywordsFromURL(String url) {
        try {
            URI uri = new URI(url);
            String host = uri.getHost(); // Extract the host
            String path = uri.getPath(); // Extract the path
            String query = uri.getQuery(); // Extract the query

            // Extract keywords from the host
            List<String> hostKeywords = new ArrayList<>();
            if (host != null) {
                hostKeywords = Arrays.asList(host.split("\\."));
                hostKeywords = hostKeywords.stream()
                        .filter(k -> !k.isEmpty() && !isCommonTLD(k)) // Remove empty parts and common TLDs
                        .collect(Collectors.toList());
            }
            //logger.info("hostKeywords: " + hostKeywords);

            // Extract keywords from the path
            List<String> pathKeywords = Arrays.asList(path.split("/"));
            pathKeywords = pathKeywords.stream()
                    .filter(k -> !k.isEmpty()) // Remove empty segments
                    .map(segment -> segment.replaceAll("\\.html?$", "")) // Remove .html or .htm
                    .collect(Collectors.toList());
            //logger.info("pathKeywords: " + pathKeywords);

            // Extract keywords from the query
            List<String> queryKeywords = query != null
                    ? Arrays.stream(query.split("&"))
                    .map(param -> param.split("=")[0]) // Extract the parameter names
                    .filter(k -> !k.isEmpty()) // Ensure no empty keywords
                    .collect(Collectors.toList())
                    : Collections.emptyList();
            //logger.info("queryKeywords: " + queryKeywords);

            // Combine and return keywords
            return String.join(" ", hostKeywords) + " " +
                    String.join(" ", pathKeywords) + " " +
                    String.join(" ", queryKeywords);
        } catch (Exception e) {
//            logger.warn("Error extracting keywords from URL: " + url + " - " + e.getMessage());
            return "";
        }
    }

    private static boolean isCommonTLD(String segment) {
        // List of common TLDs to exclude
        Set<String> commonTLDs = Set.of(
                "com", "net", "org", "edu", "gov", "co", "uk", "io", "info", "biz", "us"
        );
        return commonTLDs.contains(segment.toLowerCase());
    }

    private static String extractCriticalPageElements(String pageContent) {
        if (pageContent == null || pageContent.isEmpty()) {
            return "";
        }

        StringBuilder criticalContent = new StringBuilder();

        try {
            // Extract <title>
            Matcher titleMatcher = Pattern.compile("<title>(.*?)</title>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(pageContent);
            if (titleMatcher.find()) {
                criticalContent.append(titleMatcher.group(1)).append(" ");
            }

            // Extract headers (<h1>, <h2>, <h3>, <h4>, <h5>)
            Matcher headerMatcher = Pattern.compile("<h[1-5]>(.*?)</h[1-5]>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(pageContent);
            while (headerMatcher.find()) {
                criticalContent.append(headerMatcher.group(1)).append(" ");
            }

            // Extract meta description
            Matcher metaDescMatcher = Pattern.compile("<meta\\s+name\\s*=\\s*['\"]description['\"]\\s+content\\s*=\\s*['\"](.*?)['\"]", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(pageContent);
            if (metaDescMatcher.find()) {
                criticalContent.append(metaDescMatcher.group(1)).append(" ");
            }

            // Extract meta keywords
            Matcher metaKeywordsMatcher = Pattern.compile("<meta\\s+name\\s*=\\s*['\"]keywords['\"]\\s+content\\s*=\\s*['\"](.*?)['\"]", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(pageContent);
            if (metaKeywordsMatcher.find()) {
                criticalContent.append(metaKeywordsMatcher.group(1)).append(" ");
            }

        } catch (Exception e) {
            Logger.getLogger(Indexer.class).warn("Error extracting critical elements: " + e.getMessage());
        }

        // Normalize and return critical content
        return criticalContent.toString()
                .replaceAll("&amp;", "&")
                .replaceAll("&lt;", "<")
                .replaceAll("&gt;", ">")
                .replaceAll("&quot;", "\"")
                .replaceAll("&apos;", "'")
                .replaceAll("&[a-zA-Z0-9#]+;", " ") // Remove other entities
                .replaceAll("\\s+", " ") // Normalize whitespace
                .trim();
    }

    private static Set<String> preprocessAndStem(String content) {
        if (content == null || content.isEmpty()) {
            return Collections.emptySet();
        }

        PorterStemmer stemmer = new PorterStemmer();
        Set<String> stemmedWords = new HashSet<>();

        String[] words = content.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                for (char ch : word.toCharArray()) {
                    stemmer.add(ch);
                }
                stemmer.stem();
                stemmedWords.add(stemmer.toString());
            }
        }

        return stemmedWords;
    }

    private static boolean isImportantWord(String word, Set<String> stemmedCriticalContent, Set<String> stemmedUrlKeywords) {
        // Check if the word exists in either of the preprocessed sets
        return stemmedCriticalContent.contains(word) || stemmedUrlKeywords.contains(word);
    }
}
