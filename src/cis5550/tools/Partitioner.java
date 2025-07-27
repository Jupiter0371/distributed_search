package cis5550.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Vector;

public class Partitioner implements Serializable {

    private static final Logger logger = Logger.getLogger(Partitioner.class);

    public class Partition implements Serializable {
        public String kvsWorker;
        public String fromKey;
        public String toKeyExclusive;
        public String assignedFlameWorker;

        Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg, String assignedFlameWorkerArg) {
            kvsWorker = kvsWorkerArg;
            fromKey = fromKeyArg;
            toKeyExclusive = toKeyExclusiveArg;
            assignedFlameWorker = assignedFlameWorkerArg;
        }

        Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg) {
            kvsWorker = kvsWorkerArg;
            fromKey = fromKeyArg;
            toKeyExclusive = toKeyExclusiveArg;
            assignedFlameWorker = null;
        }

        public String toString() {
            return "[kvs:" + kvsWorker + ", keys: " + (fromKey == null ? "null" : fromKey) + "-"
                    + (toKeyExclusive == null ? "null" : toKeyExclusive) + ", flame: " + assignedFlameWorker + "]";
        }
    };

    boolean sameIP(String a, String b) {
        String[] aPcs = a.split(":");
        String[] bPcs = b.split(":");
        return aPcs[0].equals(bPcs[0]);
    }

    Vector<String> flameWorkers;
    Vector<Partition> partitions;
    boolean alreadyAssigned;
    int keyRangesPerWorker;

    public Partitioner() {
        partitions = new Vector<Partition>();
        flameWorkers = new Vector<String>();
        alreadyAssigned = false;
        keyRangesPerWorker = 1;
    }

    public void setKeyRangesPerWorker(int keyRangesPerWorkerArg) {
        keyRangesPerWorker = keyRangesPerWorkerArg;
    }

    public void addKVSWorker(String kvsWorker, String fromKeyOrNull, String toKeyOrNull) {
        partitions.add(new Partition(kvsWorker, fromKeyOrNull, toKeyOrNull));
    }

    public void addFlameWorker(String worker) {
        flameWorkers.add(worker);
    }

    public Vector<Partition> assignPartitions() {
        if (alreadyAssigned || (flameWorkers.isEmpty()) || partitions.isEmpty())
            return null;

        Random rand = new Random();

        // calculate the number of partitions required
        int requiredNumberOfPartitions = flameWorkers.size() * this.keyRangesPerWorker;

        // let's sort the partitions by key so we can identify a partition using binary search
        partitions.sort((e1, e2) -> {
            if (e1.fromKey == null) {
                return -1;
            } else if (e2.fromKey == null) {
                return 1;
            } else {
                return e1.fromKey.compareTo(e2.fromKey);
            }
        });

        // print out the partitions
        for (Partition p : partitions) {
            logger.info(p.toString());
        }

        // create a hashset of current split points to avoid creating empty partitions (unlikely)
        HashSet<String> currSplits = new HashSet<>();
        // assume that first partition has fromKey == null
        for (int i = 1; i < partitions.size(); i++) {
            currSplits.add(partitions.elementAt(i).fromKey);
        }

        int additionalSplitsNeededPerOriginalPartition = (int) Math.ceil((double) requiredNumberOfPartitions
                / partitions.size()) - 1;

        if (additionalSplitsNeededPerOriginalPartition > 0) {
            Vector<Partition> allPartitions = new Vector<>();

            for (Partition p : partitions) {
                int count = 0;
                String fromKey = p.fromKey;
                String toKeyExclusive = p.toKeyExclusive;
                ArrayList<String> newSplits = new ArrayList<String>();

                while (count < additionalSplitsNeededPerOriginalPartition) {
                    String split;

                    do {
                        split = rand.ints(97, 123).limit(5)
                                .collect(StringBuilder::new, StringBuilder::appendCodePoint,
                                        StringBuilder::append)
                                .toString();
                    } while (currSplits.contains(split) ||
                            ((fromKey != null && split.compareTo(fromKey) < 0) ||
                            (toKeyExclusive != null && split.compareTo(toKeyExclusive) >= 0)));

                    count += 1;

                    currSplits.add(split);
                    newSplits.add(split);
                }

                newSplits.sort(String::compareTo);

                // Add the first partition
                allPartitions.add(new Partition(p.kvsWorker, fromKey, newSplits.get(0)));
                // Add the middle partitions
                for (int j = 0; j < newSplits.size() - 1; j++) {
                    allPartitions.add(new Partition(p.kvsWorker, newSplits.get(j), newSplits.get(j + 1)));
                }
                // Add the last partition
                allPartitions.add(new Partition(p.kvsWorker, newSplits.get(newSplits.size() - 1),
                        toKeyExclusive));
            }
            partitions = allPartitions;
        }

        /*
         * Now we'll try to evenly assign partitions to workers, giving preference to
         * workers on the same host
         */

        int[] numAssigned = new int[flameWorkers.size()];

        for (int i = 0; i < partitions.size(); i++) {
            int bestCandidate = 0;
            int bestWorkload = 9999;
            for (int j = 0; j < numAssigned.length; j++) {
                if ((numAssigned[j] < bestWorkload) || ((numAssigned[j] == bestWorkload)
                        && sameIP(flameWorkers.elementAt(j), partitions.elementAt(i).kvsWorker))) {
                    bestCandidate = j;
                    bestWorkload = numAssigned[j];
                }
            }

            numAssigned[bestCandidate]++;
            partitions.elementAt(i).assignedFlameWorker = flameWorkers.elementAt(bestCandidate);
        }

        /* Finally, we'll return the partitions to the caller */
        // print out the partitions
        logger.info("Partitions after assignment:");
        for (Partition p : partitions) {
            logger.info(p.toString());
        }

        alreadyAssigned = true;
        return partitions;
    }

    public static void main(String[] args) {
        Partitioner p = new Partitioner();
        p.setKeyRangesPerWorker(1);

        test1(p);
//        test2(p);
//        test3(p);
//        test4(p);
//        test5(p);

        Vector<Partition> result = p.assignPartitions();
        for (Partition x : result)
            System.out.println(x);
    }

    public static void test1(Partitioner p) {
        // 1 KVS
        p.addKVSWorker("10.0.0.1:1001", null, "baaaa");
        p.addKVSWorker("10.0.0.1:1001", "baaaa", null);

        p.addFlameWorker("10.0.0.1:2001");
        p.addFlameWorker("10.0.0.2:2002");
        p.addFlameWorker("10.0.0.3:2003");
        p.addFlameWorker("10.0.0.4:2004");
        p.addFlameWorker("10.0.0.4:2005");
    }

    public static void test2(Partitioner p) {
        // 2 KVS
        p.addKVSWorker("10.0.0.1:1001", null, "aaaaa");
        p.addKVSWorker("10.0.0.2:1002", "aaaaa", "naaaa");
        p.addKVSWorker("10.0.0.1:1001", "naaaa", null);

        p.addFlameWorker("10.0.0.1:2001");
        p.addFlameWorker("10.0.0.2:2002");
        p.addFlameWorker("10.0.0.3:2003");
        p.addFlameWorker("10.0.0.4:2004");
        p.addFlameWorker("10.0.0.4:2005");
    }

    public static void test3(Partitioner p) {
        // 3 KVS
        p.addKVSWorker("10.0.0.1:1001", null, "aaaaa");
        p.addKVSWorker("10.0.0.2:1002", "aaaaa", "naaaa");
        p.addKVSWorker("10.0.0.3:1003", "naaaa", "taaaa");
        p.addKVSWorker("10.0.0.4:1001", "taaaa", null);

        // Add Flame workers
        p.addFlameWorker("10.0.0.1:2001");
        p.addFlameWorker("10.0.0.2:2002");
        p.addFlameWorker("10.0.0.3:2003");
        p.addFlameWorker("10.0.0.4:2004");
        p.addFlameWorker("10.0.0.4:2005");
    }

    public static void test4(Partitioner p) {
        // 4 KVS
        p.addKVSWorker("10.0.0.1:1001", null, "ggggg");  // Last worker, wrapping to "ggggg"
        p.addKVSWorker("10.0.0.2:1002", "ggggg", "mmmmm"); // First worker
        p.addKVSWorker("10.0.0.3:1003", "mmmmm", "sssss");
        p.addKVSWorker("10.0.0.4:1004", "sssss", "xxxxx");
        p.addKVSWorker("10.0.0.1:1001", "xxxxx", null);  // Last worker, wrapping to "null"

        p.addFlameWorker("10.0.0.1:2001");
        p.addFlameWorker("10.0.0.2:2002");
        p.addFlameWorker("10.0.0.3:2003");
        p.addFlameWorker("10.0.0.4:2004");
    }

    public static void test5(Partitioner p) {
        p.addKVSWorker("10.0.0.1:1001", null, "baaaa");
        p.addKVSWorker("10.0.0.2:1002", "baaaa", "kmaaa");
        p.addKVSWorker("10.0.0.3:1003", "kmaaa", "taaab");
        p.addKVSWorker("10.0.0.4:1001", "taaab", null);

        p.addFlameWorker("10.0.0.1:2001");
        p.addFlameWorker("10.0.0.2:2002");
        p.addFlameWorker("10.0.0.3:2003");
        p.addFlameWorker("10.0.0.4:2004");
        p.addFlameWorker("10.0.0.4:2005");

    }
}
