// package edu.upenn.cis.cis455;

// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.Comparator;
// import java.util.HashMap;
// import java.util.LinkedHashMap;
// import java.util.List;
// import java.util.Map.Entry;

// public class SpellCheck {

// 	Trie trie;

// 	class TrieNode {
// 		TrieNode[] nodes = new TrieNode[26];
// 		int count;
// 		boolean isEnd;

// 		public int getValue() {
// 			return count;
// 		}

// 		public void incrementValue() {
// 			count++;
// 		}
// 	}

// 	public SpellCheck() {
// 		BufferedReader br;
// 		Trie trie = new Trie();
// 		try {
// 			br = new BufferedReader(new FileReader("no_stop_words.txt"));
// 			String line = br.readLine();
// 			while (line != null) {
// 				trie.add(line);
// 				line = br.readLine();
// 			}
// 			br.close();
// 		} catch (IOException e) {
// 			e.printStackTrace();
// 		}
// 		this.trie = trie;
// 	}


// 	public String spellChecking(String word) {
// 		if (inputWord.length()==0 || inputWord==null || invalid.contains(inputWord.toLowerCase())) {
// 			return null;
// 		}
// 		String s = word.toLowerCase();
// 		String res = null;
// 		TreeMap<Integer, TreeMap<Integer, TreeSet<String>>> map = new TreeMap<>();
// 		TrieNode node = this.trie.find(s);
// 		if(node == null) { 
// 			for (String w: dict.keySet()) {
// 				int dist = editDistance(w, s);
// 				TreeMap<Integer, TreeSet<String>> similarWords = map.getOrDefault(dist, new TreeMap<>());
// 				int freq = dict.get(w);
// 				TreeSet<String> set = similarWords.getOrDefault(freq, new TreeSet<>());
// 				set.add(w);   
// 				similarWords.put(freq, set);
// 				map.put(dist, similarWords);  
// 			}
// 			res = map.firstEntry().getValue().lastEntry().getValue().first();
// 		} else if (node != null) {
// 			res = s;
// 		}
// 		return res;
// 	}

// 	public int editDistance(String word1, String word2) {
// 		int n = word1.length();
// 		int m = word2.length();
// 		int dp[][] = new int[n+1][m+1];

// 		for (int i = 0; i <= n; i++) {
// 			for (int j = 0; j <= m; j++) {
// 				if (i == 0) {
// 					dp[i][j] = j;
// 				} else if (j == 0) {
// 					dp[i][j] = i;
// 				} else if (word1.charAt(i-1) == word2.charAt(j-1)) {
// 					dp[i][j] = dp[i-1][j-1];
// 				} else if (i > 1 && j > 1 && word1.charAt(i-1) == word2.charAt(j-2) && word1.charAt(i-2) == word2.charAt(j-1)) {
// 					dp[i][j] = 1 + Math.min(Math.min(dp[i-2][j-2], dp[i-1][j]), Math.min(dp[i][j-1], dp[i-1][j-1]));
// 				} else {
// 					dp[i][j] = 1 + Math.min(dp[i][j-1], Math.min(dp[i-1][j], dp[i-1][j-1]));
// 				}
// 			}
// 		}
// 		return dp[n][m];
// 	}
// }