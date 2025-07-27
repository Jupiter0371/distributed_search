package cis5550.tools;
import java.io.*;
import java.util.*;

public class SpellChecker {
    public Set<String> dictionary = new HashSet<>();

    public void loadDictionary(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                dictionary.add(line.trim().toLowerCase());
            }
        }
    }

    public boolean isWordCorrect(String word) {
        return dictionary.contains(word.toLowerCase());
    }

    public String suggestCorrections(String word) {
        List<String> suggestions = new ArrayList<>();
        for (String dictWord : dictionary) {
            if (getEditDistance(word, dictWord) <= 1) { // Threshold for similarity
                suggestions.add(dictWord);
            }
        }
        if (suggestions.isEmpty()) {
            return word;
        }
        // randomize the suggestions
        return suggestions.get(new Random().nextInt(suggestions.size()));
    }

    private int getEditDistance(String word1, String word2) {
        int[][] dp = new int[word1.length() + 1][word2.length() + 1];
        for (int i = 0; i <= word1.length(); i++) {
            for (int j = 0; j <= word2.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = Math.min(dp[i - 1][j - 1]
                                    + (word1.charAt(i - 1) == word2.charAt(j - 1) ? 0 : 1),
                            Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
                }
            }
        }
        return dp[word1.length()][word2.length()];
    }

    public static void main(String[] args) throws IOException {
        SpellChecker checker = new SpellChecker();
        checker.loadDictionary("words.txt");

        String testWord = "universaty";
        if (checker.isWordCorrect(testWord)) {
            System.out.println(testWord + " is spelled correctly.");
        } else {
            System.out.println(testWord + " is misspelled. Suggestions: " + checker.suggestCorrections(testWord));
        }
    }
}