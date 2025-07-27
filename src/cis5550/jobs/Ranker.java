package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.SpellChecker;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Ranker {

    private static final Logger logger = Logger.getLogger(Ranker.class);
    private static final String delimiter = "\u0001";
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
            "can", "will", "just", "don", "should", "now"));

    private static final int MAX_CACHE_SIZE = 100;
    private static final Map<String, List<SearchResult>> cachedQuery = new LinkedHashMap<>(MAX_CACHE_SIZE, 0.75f,
            true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<SearchResult>> eldest) {
            return size() > MAX_CACHE_SIZE;
        }
    };

    public record SearchResult(String url, double score) {
    }

    public record SearchList(List<SearchResult> searchResults, int totalResults) {
    }

    public static SearchList processQuery(KVSClient kvs, String query, int pageNum, int pageSize,
            SpellChecker spellChecker) throws Exception {
        query = query.strip();
        if (query == null || query.isEmpty())
            return new SearchList(new ArrayList<>(), 0);

        // Normalize query
        List<String> queryTerms = normalizeQuery(query);
        if (queryTerms.isEmpty())
            return new SearchList(new ArrayList<>(), 0);

        PorterStemmer stemmer = new PorterStemmer();
        StringBuilder stemmedQuery = new StringBuilder();
        Map<String, Row> termToRowMap = new HashMap<>();

        // extract 1-gram terms
        List<String> stemmedTerms = new ArrayList<>();
        for (String queryTerm : queryTerms) {
            for (char ch : queryTerm.toCharArray()) {
                stemmer.add(ch);
            }
            stemmer.stem();
            String stemmedTerm = stemmer.toString();

            Row row = kvs.getRow("pt-index", stemmedTerm);
            boolean spellCorrect = true;
            // spell check
            if (row == null) {
                spellCorrect = false;
                for (int i = 0; i < 5; i++) {
                    String suggestion = spellChecker.suggestCorrections(stemmedTerm);
                    String suggestedWord = suggestion.toLowerCase();

                    // stemming the suggested word
                    for (char ch : suggestedWord.toCharArray())
                        stemmer.add(ch);
                    stemmer.stem();
                    suggestedWord = stemmer.toString();

                    // check if the suggested word is in the indexer table
                    row = kvs.getRow("pt-index", suggestedWord);
                    if (row != null) {
                        spellCorrect = true;
                        termToRowMap.put(suggestedWord, row);
                        logger.info("spell correction done.");
                        break;
                    }
                }
            } else {
                termToRowMap.put(stemmedTerm, row);
            }

            if (spellCorrect) {
                stemmedTerms.add(stemmedTerm);
                stemmedQuery.append(stemmedTerm).append(" ");
            }
        }

        if (stemmedTerms.isEmpty()) {
            logger.warn("No stemmed query terms found for the query: " + query);
            return new SearchList(new ArrayList<>(), 0);
        }

        // check if the query is cached
        String stemmedQueryStripped = stemmedQuery.toString().strip();
        if (cachedQuery.containsKey(stemmedQueryStripped)) {
            List<SearchResult> cachedResults = cachedQuery.get(stemmedQueryStripped);
            List<SearchResult> results = new ArrayList<>();
            int start = (pageNum - 1) * pageSize;
            int end = Math.min(start + pageSize, cachedResults.size());

            for (int i = start; i < end; i++) {
                results.add(cachedResults.get(i));
            }

            logger.info("Returning cached results for query: " + query);
            return new SearchList(results, cachedResults.size());
        }

        // Extract bi-gram phrases
        List<String> phrases = new ArrayList<>();
        for (int i = 0; i < stemmedTerms.size() - 1; i++) {
            phrases.add(stemmedTerms.get(i) + "_" + stemmedTerms.get(i + 1));
        }

        // Match consecutive positions for phrases
        Map<String, Double> urlToPhraseScore = new HashMap<>();
        for (String phrase : phrases) {
            String[] words = phrase.split("_");
            Row row1 = termToRowMap.get(words[0]);
            Row row2 = termToRowMap.get(words[1]);
            // logger.info("row1"+row1);
            // logger.info("row2"+row2);

            if (row1 != null && row2 != null) {
                Map<String, List<Integer>> positions1 = parsePositions(row1.get("acc"));
                Map<String, List<Integer>> positions2 = parsePositions(row2.get("acc"));
                logger.info("position1" + positions1);
                logger.info("positions2" + positions2);

                for (String url : positions1.keySet()) {
                    if (positions2.containsKey(url)) {
                        List<Integer> pos1 = positions1.get(url);
                        List<Integer> pos2 = positions2.get(url);
                        logger.info("pos1" + pos1.toString());
                        logger.info("pos2" + pos2.toString());

                        for (int p1 : pos1) {
                            if (pos2.contains(p1 + 1)) { // Consecutive positions
                                urlToPhraseScore.merge(url, 1.0, Double::sum);
                            }
                        }
                    }
                }
            }
        }

        logger.info("urlToPhraseScore" + urlToPhraseScore.toString());

        // Compute TF-IDF scores
        Map<String, Double> relativeFrequencyMap = calculateRelativeFrequencyMap(stemmedTerms);
        Map<String, Double> urlToTFIDF = findTFIDFScore(stemmedTerms, termToRowMap, relativeFrequencyMap);

        // Compute PageRank scores
        Map<String, Double> urlToPageRank = findPageRankScore(kvs, urlToTFIDF);

        // Combine scores
        Map<String, Double> urlToFinalScore = new HashMap<>();
        double alpha = 0.7, beta = 0.3, gamma = 0.7; // Weights for TF-IDF, PageRank, and phrase matches

        for (String url : urlToTFIDF.keySet()) {
            double TFIDF = urlToTFIDF.getOrDefault(url, 0.0);
            double PageRank = urlToPageRank.getOrDefault(url, 0.0);
            double PhraseBoost = urlToPhraseScore.getOrDefault(url, 0.0);
            double finalScore = alpha * TFIDF + beta * PageRank + gamma * PhraseBoost;
            logger.info("url" + url + "TFIDF" + TFIDF + "PageRank" + PageRank + "PhraseBoost" + Double.max(PhraseBoost, 3)
                    + "FinalScore" + finalScore);
            urlToFinalScore.put(url, finalScore);
        }

        List<SearchResult> results = urlToFinalScore.entrySet().stream()
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()))
                .map(e -> new SearchResult(e.getKey(), e.getValue()))
                .toList();
        
        cachedQuery.put(stemmedQueryStripped, results);
        int start = (pageNum - 1) * pageSize;
        int end = Math.min(start + pageSize, results.size());
        return new SearchList(results.subList(start, end), results.size());
    }

    private static Map<String, List<Integer>> parsePositions(String acc) {
        Map<String, List<Integer>> urlPositions = new HashMap<>();
        if (acc == null)
            return urlPositions;

        String[] entries = acc.split(delimiter);
        for (String entry : entries) {
            String[] parts = entry.split(delimiter3);
            if (parts.length >= 2) {
                String[] potentialSub = parts[0].split(delimiter4);
                String url = potentialSub[potentialSub.length - 1];
                List<Integer> positions = Arrays.stream(parts[1].split(" "))
                        .map(Integer::parseInt)
                        .toList();
                urlPositions.put(url, positions);
            }
        }
        return urlPositions;
    }

    private static Map<String, Double> findTFIDFScore(List<String> stemmedQueryTerms,
            Map<String, Row> stemmedQueryTermsRowsMap,
            Map<String, Double> relativeFrequencyMap) {
        // Map to store final TF-IDF scores for each URL
        Map<String, Double> urlToTFIDF = new HashMap<>();
        // Map to store cumulative tf^2 for normalization
        Map<String, Double> urlToNorm = new HashMap<>();
        List<Row> stemmedQueryTermsRows = new ArrayList<>(stemmedQueryTermsRowsMap.values());

        // Compute TF-IDF scores for each URL
        for (int i = 0; i < stemmedQueryTerms.size(); i++) {
            // Get the row for the term
            Row row = stemmedQueryTermsRows.get(i);
            String rowValue = row.get("acc");

            // Skip if the row is null
            if (rowValue == null)
                continue;

            // Parse the total number of documents
            long N = Long.parseLong(rowValue.split(delimiter4)[0]);
            // Parse the complete string of URL entries
            String URLValues = rowValue.split(delimiter4)[1];
            // Get the list of URL entries
            String[] entries = URLValues.split(delimiter);
            // Get the number of documents containing the term
            int n = entries.length;

            // Compute the IDF and term TF
            double idf = Math.log((double) N / n);
            double termTf = relativeFrequencyMap.getOrDefault(stemmedQueryTerms.get(i), 0.0);

            // Accumulate tf² for normalization
            Arrays.stream(entries)
                    .map(entry -> entry.split(delimiter3))
                    .forEach(parts -> {
                        String url = parts[0];
                        double tf;
                        try {
                            tf = Double.parseDouble(parts[3]);
                        } catch (NumberFormatException e) {
                            System.out.println("Error parsing TF for URL: " + url);
                            return;
                        }

                        // Accumulate tf² for normalization
                        urlToNorm.merge(url, tf * tf, Double::sum);

                        // Add partial TF-IDF score
                        double partialScore = tf * termTf * idf;
                        urlToTFIDF.merge(url, partialScore, Double::sum);
                    });
        }

        // Normalize and compute final TF-IDF scores
        for (Map.Entry<String, Double> entry : urlToTFIDF.entrySet()) {
            String url = entry.getKey();
            double score = entry.getValue();
            double norm = Math.sqrt(urlToNorm.getOrDefault(url, 1.0)); // Use 1.0 as default if no norm exists

            if (norm > 0) {
                urlToTFIDF.put(url, score / norm);
            } else {
                System.out.println("Warning: Norm is 0 for URL: " + url);
                urlToTFIDF.put(url, 0.0); // Avoid division by zero
            }
        }

        return urlToTFIDF;
    }

    private static Map<String, Double> findPageRankScore(KVSClient kvs, Map<String, Double> urls) throws IOException {
        Map<String, Double> urlToPageRank = new HashMap<>();
        for (String url : urls.keySet()) {
            String hashedUrl = Hasher.hash(url);
            Row row = kvs.getRow("pt-pageranks", hashedUrl);
            if (row != null)
                urlToPageRank.put(url, Double.parseDouble(row.get("rank")));
            else
                urlToPageRank.put(url, 0.0);
        }
        return urlToPageRank;
    }

    private static List<String> normalizeQuery(String query) {
        // Split the query into terms, convert to lowercase, and filter out stop words
        String normalizedQuery = query.toLowerCase()
                .replaceAll("(?s)<script.*?>.*?</script>", " ") // Remove script contents (if applicable)
                .replaceAll("(?s)<style.*?>.*?</style>", " ") // Remove style contents (if applicable)
                .replaceAll("<[^>]*>", " ") // Remove HTML tags (if any)
                .replaceAll("&amp;", "&")
                .replaceAll("&lt;", "<")
                .replaceAll("&gt;", ">")
                .replaceAll("&quot;", "\"")
                .replaceAll("&apos;", "'")
                .replaceAll("&[a-zA-Z0-9#]+;", " ") // Decode other HTML entities
                .replaceAll("[.,:;!?'\"()\\[\\]{}<>]", " ") // Remove punctuation
                .replaceAll("\\b-\\b", " ") // Remove isolated hyphens
                .replaceAll("(?<=\\b)'|'(?=\\b)", "") // Remove leading/trailing apostrophes
                .replaceAll("\\s+", " ") // Normalize whitespace
                .trim();

        // Remove stop words
        return Arrays.stream(normalizedQuery.toLowerCase().split("\\s+"))
                .filter(term -> !stopWords.contains(term)) // Remove stop words
                .toList();
    }

    private static Map<String, Double> calculateRelativeFrequencyMap(List<String> terms) {
        Map<String, Integer> frequencyMap = new HashMap<>();
        for (String term : terms)
            frequencyMap.merge(term, 1, Integer::sum);

        int maxFrequency = Collections.max(frequencyMap.values());
        return frequencyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> 0.5 + 0.5 * e.getValue() / maxFrequency));
    }
}
