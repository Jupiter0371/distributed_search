package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static cis5550.jobs.Crawler.extractURLs;

public class PageRank {

    private static final Logger logger = Logger.getLogger(PageRank.class);

    private static final String delimiter = "\u0001";
    private static final double decayFactor = 0.85;

    public static class IterationResult {
        double maxRankChange;
        double convergenceRatio;

        public IterationResult(double maxRankChange, double convergenceRatio) {
            this.maxRankChange = maxRankChange;
            this.convergenceRatio = convergenceRatio;
        }
    }

    public static class PageRankState {
        public double rc;
        public double rp;
        public List<String> links;

        public PageRankState(double rc, double rp, List<String> links) {
            this.rc = rc;
            this.rp = rp;
            this.links = links;
        }

        public static PageRankState fromString(String value) {
            try {
                String[] parts = value.split(",", 3);
                if (parts.length < 3) {
                    logger.warn("Invalid PageRankState input: " + value);
                    return null;
                }

                double rc = Double.parseDouble(parts[0]);
                double rp = Double.parseDouble(parts[1]);
                List<String> links = parts[2].isEmpty() ? Collections.emptyList() :
                        Arrays.asList(parts[2].split(","));
                return new PageRankState(rc, rp, links);
            } catch (Exception e) {
                logger.error("Error parsing PageRankState from: " + value, e);
                return null;
            }
        }

        public String toString() {
            return rc + "," + rp + "," + String.join(",", links);
        }
    }

    public static void run(FlameContext context, String[] args) throws Exception {
        if (args.length > 2) {
            context.output("Usage: PageRank [threshold] [percentage]\n");
            return;
        }

        // Process the arguments (threshold, percentage)
        Double[] params = processArgs(context, args);
        double convergenceThreshold = params[0];
        double convergencePercentage = params[1];

        logger.info("Starting PageRank job...\n");

        // Initialize the PageRank values
        pageRankInitialization(context);

        // Compute count
        FlameRDD rdd = context.fromTable("pagerank_state", row -> {
            if (row.key() != null && row.get("acc") != null) {
                // Get the ranks
                String ranks = new String(row.getBytes("acc"), StandardCharsets.UTF_8).replace(delimiter, " ");
                return row.key() + delimiter + ranks;
            } else {
                return null;
            }
        });
        FlameRDD rddFiltered = rdd.filter(Objects::nonNull);
        long totalUrls = rddFiltered.count();

        // Delete intermediate tables
        context.getKVS().delete(rdd.getTableName());
        context.getKVS().delete(rddFiltered.getTableName());

        int iteration = 0;
        double maxRankChange = Double.MAX_VALUE;
        double convergenceRatio = 0.0;

        while (maxRankChange >= convergenceThreshold && convergenceRatio < convergencePercentage) {
            logger.info("Starting PageRank iteration " + (iteration + 1) + "...\n");
            IterationResult result = pageRankIteration(context, convergenceThreshold, totalUrls);
            maxRankChange = result.maxRankChange;
            convergenceRatio = result.convergenceRatio;
            logger.info("Iteration " + (iteration + 1) + " complete. " +
                    "Max rank change: " + maxRankChange + ", Convergence ratio: " + convergenceRatio + "%\n");
            iteration++;
        }

        context.output("PageRank converged after " + iteration + " iterations.\n");

        // Save the final PageRank values
        pageRankFinalization(context);

        context.output("PageRank complete.");
    }

    public static void pageRankInitialization(FlameContext context) throws Exception {
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

        // Filter out null values
        FlameRDD rddFiltered = rdd.filter(Objects::nonNull);
        context.getKVS().delete(rdd.getTableName());

        // Convert RDD to PairRDD of (urlHash, "1.0,1.0,L") (multiple columns)
        FlamePairRDD urlVals = rddFiltered.flatMapToPair(s -> {
            String[] parts = s.split(delimiter, 2);
            if (parts.length != 2) {
                return Collections.emptyList();
            }
            String url = parts[0];
            String pageContent = parts[1];

            // Compute hash of the URL
            String urlHash = Hasher.hash(url);

            // Extract and normalize links from the page content
            List<String> links = extractURLs(pageContent, url);

            // Normalize links and compute their hashes
            List<String> linkHashes = new ArrayList<>();
            for (String link : links) {
                String linkHash = Hasher.hash(link);
                linkHashes.add(linkHash);
            }

            // Create the value "1.0,1.0,L"
            String L = String.join(",", linkHashes);
            String value = "1.0,1.0," + L;

            return Collections.singletonList(new FlamePair(urlHash, value));
        });
        context.getKVS().delete(rddFiltered.getTableName());

        // Aggregate the PairRDD to (urlHash, "1.0,1.0,L")
        FlamePairRDD pageRankIndex = urlVals.foldByKey("", (accum, url) -> {
            if (accum.isEmpty()) {
                return url;
            } else if (!accum.contains(url)) {
                return accum + "," + url;
            } else {
                // Avoid duplicate URLs
                return accum;
            }
        });
        context.getKVS().delete(urlVals.getTableName());

        // Save the state table as 'pagerank_state'
        pageRankIndex.saveAsTable("pagerank_state");
    }

    public static IterationResult pageRankIteration(FlameContext context, double convergenceThreshold, long totalUrls) throws Exception {
        // Load the 'pt-pageranks' table
        FlameRDD rdd = context.fromTable("pagerank_state", row -> {
            if (row.key() != null && row.get("acc") != null) {
                // Get the ranks
                String ranks = new String(row.getBytes("acc"), StandardCharsets.UTF_8).replace(delimiter, " ");
                return row.key() + delimiter + ranks;
            } else {
                return null;
            }
        });

        // Filter out null values
        FlameRDD rddFiltered = rdd.filter(Objects::nonNull);
        context.getKVS().delete(rdd.getTableName());

        // Convert RDD to PairRDD of (url, "rc,rp,L")
        FlamePairRDD urlHashValuePairRDD = rddFiltered.flatMapToPair(s -> {
            String[] parts = s.split(delimiter, 2);
            if (parts.length == 2) {
                return Collections.singletonList(new FlamePair(parts[0], parts[1]));
            } else {
                logger.warn("Skipping invalid record: " + s);
                return Collections.emptyList();
            }
        });
        context.getKVS().delete(rddFiltered.getTableName());

        // Compute the transfer table (url, "v")
        FlamePairRDD transferTable = urlHashValuePairRDD.flatMapToPair(pair -> {
            String u = pair._1();
            String value = pair._2();

            // Parse the value
            PageRankState state = PageRankState.fromString(value);
            if (state == null) {
                logger.warn("Skipping null PageRankState for key: " + u);
                return Collections.emptyList();
            }

            double rc = state.rc;
            List<String> links = state.links;

            // Remove duplicates and store it in a set
            Set<String> linkSet = new HashSet<>(links);
            // Get the number of links
            int n = linkSet.size();

            List<FlamePair> outputs = new ArrayList<>();

            if (n > 0) {
                double v = decayFactor * rc / n;
                for (String li : linkSet) {
                    outputs.add(new FlamePair(li, String.valueOf(v)));
                }
            }

            // Send rank 0.0 from u to itself
            if (!linkSet.contains(u)) {
                outputs.add(new FlamePair(u, "0.0"));
            }

            return outputs;
        });

        // Aggregate the transfer table (url, "sum of vi")
        FlamePairRDD aggregatedTransferTable = transferTable.foldByKey("0.0", (accum, v) -> {
            try {
                double sum = Double.parseDouble(accum) + Double.parseDouble(v);
                return Double.toString(sum);
            } catch (NumberFormatException e) {
                logger.error("Error parsing double: " + accum + " or " + v, e);
                return accum; // Return the accumulator as is in case of error
            }
        });
        context.getKVS().delete(transferTable.getTableName());

        // Join the aggregated transfer table with the page rank table (url, "v,rc,rp,L")
        FlamePairRDD joinedTables = aggregatedTransferTable.join(urlHashValuePairRDD);
        context.getKVS().delete(urlHashValuePairRDD.getTableName());
        context.getKVS().delete(aggregatedTransferTable.getTableName());

        // Update the state table (url, "new_rc,new_rp,L") (multiple columns)
        FlamePairRDD stateTable = joinedTables.flatMapToPair(pair -> {
            String u = pair._1();
            String joinedValue = pair._2();

            // Split the joined value into sum_vi and the old state
            String[] joinedParts = joinedValue.split(",", 2);
            if (joinedParts.length < 2) {
                logger.warn("Invalid joined value for key " + u + ": " + joinedValue);
                return Collections.emptyList();
            }
            double sum_vi = Double.parseDouble(joinedParts[0]);
            String oldState = joinedParts[1];

            // Parse the old state
            PageRankState oldStateObj = PageRankState.fromString(oldState);
            if (oldStateObj == null) {
                logger.warn("Skipping null PageRankState for key: " + u);
                return Collections.emptyList();
            }

            double old_rc = oldStateObj.rc;
            double old_rp = oldStateObj.rp;
            List<String> links = oldStateObj.links;

            // Update the ranks
            double new_rc = sum_vi + (1 - decayFactor);

            // Create the new state value: "new_rc,new_rp,L"
            String newStateValue = new_rc + "," + old_rc + "," + String.join(",", links);

//            logger.info("In flatMapToPair: new_rc: " + new_rc + ", new_rp: " + old_rc);

            // Emit the updated state
            return Collections.singletonList(new FlamePair(u, newStateValue));
        });
        context.getKVS().delete(joinedTables.getTableName());

        // Aggregate the updated state table (url, "new_rc,new_rp,L")
        FlamePairRDD pageRankIndex = stateTable.foldByKey("", (accum, value) -> {
            return value;
        });
        context.getKVS().delete(stateTable.getTableName());

        // Compute the rank change for each url (url, "rankChange")
        FlameRDD rankChanges = pageRankIndex.flatMap(pair -> {
            String value = pair._2();

            // Parse the value to get new_rc and new_rp
            String[] parts = value.split(",", 3);
            if (parts.length < 3) {
                logger.warn("Invalid value for key " + pair._1() + ": " + value);
                return Collections.emptyList();
            }
            double new_rc = Double.parseDouble(parts[0]);
            double new_rp = Double.parseDouble(parts[1]);

            // Calculate the rank change
            double rankChange = Math.abs(new_rc - new_rp);

            // Emit the rank change
            return Collections.singletonList(String.valueOf(rankChange));
        });

        String maxRankChangeStr = rankChanges.fold("0.0", (accum, value) -> {
            try {
                double max = Double.parseDouble(accum);
                double rankChange = Double.parseDouble(value);

                // Return the maximum of the two
                return String.valueOf(Math.max(max, rankChange));
            } catch (Exception e) {
                // Log the error and return the accumulated value unchanged
                logger.error("Error during fold operation: ", e);
                return accum;
            }
        });
        context.getKVS().delete(rankChanges.getTableName());

        // Count the number of URLs that have converged and the total number of URLs
        FlameRDD rankChangesFiltered = rankChanges.filter(s -> Double.parseDouble(s) <= convergenceThreshold);
        long convergedUrls = rankChangesFiltered.count();
        context.getKVS().delete(rankChangesFiltered.getTableName());

        double maxRankChange = Double.parseDouble(maxRankChangeStr);
        double convergenceRatio = ((double) convergedUrls / totalUrls) * 100.0;
        logger.info("Convergence ratio: " + convergenceRatio + "%");
        logger.info("Max rank change: " + maxRankChange);

        // Save the updated state table as 'pagerank_state'
        context.getKVS().delete("pagerank_state");
        pageRankIndex.saveAsTable("pagerank_state");

        return new IterationResult(maxRankChange, convergenceRatio);
    }

    public static void pageRankFinalization(FlameContext context) throws Exception {
        // Delete the 'pt-pageranks' table
        context.getKVS().delete("pt-pageranks");

        // Load the 'pt-pageranks' table
        FlameRDD rdd = context.fromTable("pagerank_state", row -> {
            if (row.key() != null && row.get("acc") != null) {
                // Get the ranks
                String ranks = new String(row.getBytes("acc"), StandardCharsets.UTF_8).replace(delimiter, " ");
                return row.key() + delimiter + ranks;
            } else {
                return null;
            }
        });

        // Filter out null values
        FlameRDD rddFiltered = rdd.filter(Objects::nonNull);
        context.getKVS().delete(rdd.getTableName());

        // Convert RDD to PairRDD of (url, "rc,rp,L") (multiple columns)
        // Process each entry to save the final rank (url, "rc,rp,L")
        FlamePairRDD urlPageRank = rddFiltered.flatMapToPair(s -> {
            String[] parts = s.split(delimiter, 2);
            if (parts.length == 2) {
                return Collections.singletonList(new FlamePair(parts[0], parts[1]));
            } else {
                logger.warn("Skipping invalid record: " + s);
                return Collections.emptyList();
            }
        });
        context.getKVS().delete(rddFiltered.getTableName());

        FlamePairRDD rankTable = urlPageRank.flatMapToPair(pair -> {
            String urlHash = pair._1();
            String value = pair._2();

            // Parse the value to get the final rank
            String[] parts = value.split(",", 3);
            if (parts.length < 3) {
                logger.warn("Invalid value for key " + urlHash + ": " + value);
                return Collections.emptyList();
            }
            double finalRank = Double.parseDouble(parts[0]);

            // Write to 'pt-pageranks' table: key = urlHash, column 'rank', value = finalRank
            context.getKVS().put("pt-pageranks", urlHash, "rank",
                    String.valueOf(finalRank).getBytes(StandardCharsets.UTF_8));

            return Collections.emptyList();
        });
        context.getKVS().delete(urlPageRank.getTableName());

        // Delete intermediate tables
        context.getKVS().delete(rankTable.getTableName());
    }

    public static Double[] processArgs(FlameContext context, String[] args) {
        double threshold = 0.01;
        double percentage = 100;

        if (args.length >= 1) {
            try {
                double t = Double.parseDouble(args[0]);
                if (t > 0) {
                    threshold = t;
                } else {
                    context.output("Invalid threshold value. Using default threshold: " + threshold + "\n");
                }
            } catch (NumberFormatException e) {
                context.output("Invalid threshold value. Using default threshold: " + threshold + "\n");
            }
        }

        if (args.length == 2) {
            try {
                double p = Double.parseDouble(args[1]);
                if (p > 0 && p <= 100) {
                    percentage = p;
                } else {
                    context.output("Invalid percentage value. Using default percentage: " + percentage + "\n");
                }
            } catch (NumberFormatException e) {
                context.output("Invalid percentage value. Using default percentage: " + percentage + "\n");
            }
        }

        logger.info("Using threshold: " + threshold + ", percentage: " + percentage + "%");

        return new Double[]{threshold, percentage};
    }
}
