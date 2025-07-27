package cis5550.test;

import cis5550.flame.FlameContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameFold {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Syntax: FlameFold <inputString>");
            return;
        }

        // Split the single input string into words
        List<String> list = Arrays.asList(args[0].split(" "));

        // Use parallelize to create an RDD and perform the fold operation
        String result = ctx.parallelize(list)
                .fold("", (acc, value) -> acc + value);

        // Output the result
        ctx.output(result);
    }
}
