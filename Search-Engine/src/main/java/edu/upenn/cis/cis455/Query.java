package edu.upenn.cis.cis455;

import spark.Request;
import spark.Response;
import spark.Route;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import com.sleepycat.json_simple.JsonArray;
import com.google.common.collect.Iterables;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.PriorityQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.time.Instant;
import java.time.Duration;

import opennlp.tools.stemmer.PorterStemmer;

public class Query implements Route {

    private static final Logger logger = LogManager.getLogger(Query.class);

    private Table documentTable;
    private Table indexTable;
    private Table pagerankTable;

    private DynamoDB dynamoDB;

    private int displayLimit = 20;

    public Query(Table documentTable, Table indexTable, Table pagerankTable, DynamoDB dynamoDB) {
        this.documentTable = documentTable;
        this.indexTable = indexTable;
        this.pagerankTable = pagerankTable;
        this.dynamoDB = dynamoDB;
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        String query = request.queryParams("query");

        logger.debug("handling query: " + query);

        String queryResult = getQuery(query);
        if (queryResult == null) {
            logger.error("the returned query result is empty!");
            return new JsonArray().toJson();
        }

        logger.debug("the query result is: " + queryResult);
        return queryResult;
    }

    public Item getDocument(String documentID) {
        Item item = documentTable.getItem("url", documentID);
        return item;
    }
    
    public class Preprocess {
        PorterStemmer stemmer = new PorterStemmer();
        String[] stopWords = new String[] { "a", "able", "about", "above", "according", "accordingly", "across", "actually",
                "after", "afterwards", "again", "against", "all", "allow", "allows", "almost", "alone", "along", "already",
                "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow",
                "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate",
                "are", "aren't", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away",
                "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
                "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between",
                "beyond", "both", "brief", "but", "by", "c", "came", "can", "cannot", "cant", "can't", "cause", "causes",
                "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently",
                "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldn't",
                "course", "currently", "d", "dear", "definitely", "described", "despite", "did", "didn't", "different",
                "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "during", "e", "each", "edu", "eg",
                "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever",
                "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "f",
                "far", "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly",
                "forth", "four", "from", "further", "furthermore", "g", "get", "gets", "getting", "given", "gives", "go",
                "goes", "going", "gone", "got", "gotten", "greetings", "h", "had", "hadn't", "happens", "hardly", "has",
                "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "hello", "help", "hence", "her", "here",
                "hereafter", "hereby", "herein", "here's", "hereupon", "hers", "herself", "he's", "hi", "high", "him",
                "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "how's", "i", "i'd", "ie", "if",
                "ignored", "i'll", "i'm", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated",
                "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isn't", "it", "its", "it's", "itself",
                "i've", "j", "just", "k", "keep", "keeps", "kept", "know", "known", "knows", "l", "last", "lately", "later",
                "latter", "latterly", "least", "less", "lest", "let", "let's", "like", "liked", "likely", "little", "long",
                "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "many", "may", "maybe", "me", "mean",
                "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "mustn't", "my",
                "myself", "n", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never",
                "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not",
                "nothing", "novel", "now", "nowhere", "o", "obviously", "of", "off", "often", "oh", "ok", "okay", "old",
                "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours",
                "ourselves", "out", "outside", "over", "overall", "own", "p", "particular", "particularly", "per",
                "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "put", "q", "que",
                "quite", "qv", "r", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards",
                "relatively", "respectively", "right", "s", "said", "same", "saw", "say", "saying", "says", "second",
                "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible",
                "sent", "serious", "seriously", "seven", "several", "shall", "shan't", "she", "she'd", "she'll", "she's",
                "should", "shouldn't", "since", "six", "so", "some", "somebody", "somehow", "someone", "something",
                "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying",
                "still", "sub", "such", "sup", "sure", "t", "take", "taken", "tell", "tends", "th", "than", "thank",
                "thanks", "thanx", "that", "thats", "that's", "the", "their", "theirs", "them", "themselves", "then",
                "thence", "there", "thereafter", "thereby", "therefore", "therein", "theres", "there's", "thereupon",
                "these", "they", "they'd", "they'll", "they're", "they've", "think", "third", "this", "thorough",
                "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "tis", "to", "together",
                "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twas", "twice", "two", "u",
                "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used",
                "useful", "uses", "using", "usually", "uucp", "v", "value", "various", "very", "via", "viz", "vs", "w",
                "want", "wants", "was", "wasn't", "way", "we", "we'd", "welcome", "well", "we'll", "went", "were", "we're",
                "weren't", "we've", "what", "whatever", "what's", "when", "whence", "whenever", "when's", "where",
                "whereafter", "whereas", "whereby", "wherein", "where's", "whereupon", "wherever", "whether", "which",
                "while", "whither", "who", "whoever", "whole", "whom", "who's", "whose", "why", "why's", "will", "willing",
                "wish", "with", "within", "without", "wonder", "won't", "would", "wouldn't", "x", "y", "yes", "yet", "you",
                "you'd", "you'll", "your", "you're", "yours", "yourself", "yourselves", "you've", "z", "zero" };
        List<String> stopWordsLst = Arrays.asList(stopWords);

        public ArrayList<String> clean(String query) {
            ArrayList<String> ret = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(query);
            while (st.hasMoreTokens()) {
                String wordStr = st.nextToken();
                if (stopWordsLst.contains(wordStr)) {
                    continue;
                }
                String stemmed = stemmer.stem(wordStr);
                ret.add(stemmed);
            }
            return ret;
        }
    }

    // Struct the query term and the tfidf related to each document
    public class termTfidfMap {
        String term;
        Double tfidf;

        public termTfidfMap(String term, Double tfidf) {
            this.term = term;
            this.tfidf = tfidf;
        }
    }

    // The result of the search engine, implements comparable for convenient sorting
    public class Result implements Comparable<Result> {
        String documentID;
        Double score;

        public Result(String documentID, Double score) {
            this.documentID = documentID;
            this.score = score;
        }

        @Override
        public int compareTo(Result result) {
            if (this.score > result.score) {
                return 1;
            } else if (this.score < result.score) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public String getQuery(String query) {

        logger.debug("start handling query: lowercase and trimming: " + query);
        query = query.toLowerCase().trim();
        logger.debug("finish handling query: " + query);

        logger.debug("start preprocessing");
        // preprocessing
        Preprocess preprocess = new Preprocess();
        ArrayList<String> terms = preprocess.clean(query);
        logger.debug("finished preprocessing: " + terms);

        logger.debug("start getting query term frequency");
        // IDF for each query term
        Map<String, Double> queryTermFreq = getQueryTermFreq(terms);
        logger.debug("finished getting query term frequency: " + queryTermFreq);

        // TF-IDF index for each document that relates to the query terms
        // termTfidfMap is just another map for initializing with simplicity
        // documentID - (term, tfidf)
        Map<String, ArrayList<termTfidfMap>> docTfidfQueryMap = new HashMap<String, ArrayList<termTfidfMap>>();
        // the final set of documents to be returned / called by handle (no replications)
        Set<String> documents = new HashSet<String>();

        // the priority queue for collecting the search result
        PriorityQueue<Result> priorityqueue = new PriorityQueue<Result>();
        // the actual document in the reverse order, i.e, to be reversed
        List<String> reverseDocument = new ArrayList<String>();
        // the Json array to be returned
        JsonArray jsonArray = new JsonArray();

        logger.info("finished initialization, start computing score and getting query");
        
        for (int i = 0; i < terms.size(); i++) {
            String s = terms.get(i);
            logger.debug("At current term: " + s);

            // spec queries the table and returns an itemcollection, which can be access by the iterator
            QuerySpec spec = new QuerySpec()
                                    .withKeyConditionExpression("word = :v_word")
                                    .withValueMap(new ValueMap().withString(":v_word", s));
            ItemCollection<QueryOutcome> items = indexTable.query(spec);
            Iterator<Item> iterator = items.iterator();
            Item indexItem = null;

            // current document set that contains the query word, needed to be unioned
            // with other document set to get the documents in the above
            Set<String> currDocument = new HashSet<String>();

            while(iterator.hasNext()) {

                // each item: (word, score, url)
                indexItem = iterator.next();
                // logger.debug("Processing current item: " + indexItem);

                double tfidf = Double.valueOf((String) indexItem.get("tfidf"));
                String documentID = (String) indexItem.get("url");
                // logger.info("Tfidf: " + tfidf + "documentID: " + documentID);

                // add to the current document set
                currDocument.add(documentID);

                // current document - term - tfidf tuple
                termTfidfMap currTermTfidf = new termTfidfMap(s, tfidf);

                if (docTfidfQueryMap.containsKey(documentID)) {
                    docTfidfQueryMap.get(documentID).add(currTermTfidf);
                    // logger.debug("Current documentID found, putting " + s + " " + tfidf + " into query map");
                } else {
                    ArrayList<termTfidfMap> currDocTfIdf = new ArrayList<termTfidfMap>();
                    // insert to the array and map it to the current document / url
                    currDocTfIdf.add(currTermTfidf);
                    docTfidfQueryMap.put(documentID, currDocTfIdf);
                    // logger.debug("Current documentID not found, putting " + currDocTfIdf.toString() + " into query map");
                }
            }

            logger.info("To the end of querying indexDB");

            logger.info("start getting all of the documents");
            // get union of all of the documents
            if (i == 0) {
                documents = currDocument;
            } else {
                documents.addAll(currDocument);
            }
            logger.info(currDocument.toString());
            logger.info(documents.toString());
            logger.info("finish getting all of the documents");
        }

        // // words with few hits
        // if (documents.size() <= 3) {
        //     SpellCheck spellCheck = new SpellCheck();
        //     for (int i = 0; i < terms.size(); i++) {
        //         String s = terms.get(i);
        //         String suggestWord = spellCheck.spellChecking(s);
        //         logger.info("Suggested word is: " + suggestWord);
        //     }
        // }

        Map<String, Double> pagerankScoreMap = getPagerankScore(documents);

        // for each document that relates to the query
        int count = 0;
        int currpos = 0;

        Map<String, Item> documentItems = getDocumentBatch(documents);

        for (String document: documents) {
            double score = 0;
            // get all of the query words related and the tfidf score
            ArrayList<termTfidfMap> docInfo = docTfidfQueryMap.get(document);
            // calcualte tf-idf
            for (termTfidfMap instance: docInfo) {
                String queryTerm = instance.term;
                Double tfidf = instance.tfidf;
                Double queryWeight = queryTermFreq.get(queryTerm);
                score += tfidf * queryWeight;
            }

            // calculate page-rank score
            double pagerankScore = 0;
            if (pagerankScoreMap.get(document) == null) {
                // TODO: decide this arbitary value
                pagerankScore = 0.5;
                // logger.error(document + " is NOT FOUND in pagerank table");
                count++;
            } else {
                pagerankScore = pagerankScoreMap.get(document);
            }

            // check title weights
            Item documentItem = documentItems.get(document);
            String title = documentItem.getString("title");
            title = title.toLowerCase();
            // logger.info(title);

            if (title.contains("wikipedia")) {
                score += 0.001 * pagerankScore;
            } else {
                score += 0.2 * pagerankScore;
            }

            ArrayList<String> titleList = new ArrayList<>();
            if (title != null && !title.isBlank()) {
                titleList = preprocess.clean(title);
            }

            logger.info(titleList.toString());

            int hitNum = 1;
            for (String queryTerm: queryTermFreq.keySet()) {
                logger.info(queryTerm);
                if (titleList.contains(queryTerm)) {
                    // TODO: determin this weight
                    score += 15 * hitNum;
                    hitNum++;
                }
            }

            logger.debug(currpos + "======" + documents.size());
            currpos++;

            Result result = new Result(document, score);
            priorityqueue.add(result);
            // logger.info("Current comparison: ");
            // logger.info(priorityqueue.size() + " vs " + displayLimit);

            if (priorityqueue.size() > displayLimit) {
                priorityqueue.poll();
            }
        }

        for (Result r: priorityqueue) {
            logger.info(r.documentID + " -- " + r.score);
        }

        logger.info("total documents: " + documents.size() + " missed number: " + count);

        if (documents.size() < displayLimit) {
            displayLimit = documents.size();
        }

        for (int i = 0; i < displayLimit; i++) {
            // get document from small score to large score
            Result result = priorityqueue.poll();
            Item documentItem = getDocument(result.documentID);
            reverseDocument.add(documentItem.toJSON());
        }

        // reverse in-place
        Collections.reverse(reverseDocument);
        jsonArray.addAll(reverseDocument);

        return jsonArray.toJson();
    }

    public Map<String, Double> getPagerankScore(Set<String> documents) {
        logger.info("start getting pagerank using batchGet");
        int count = 0;
        Map<String, Double> pagerankScore = new HashMap<String, Double>();
        String pagerankTableName = this.pagerankTable.getTableName();

        // Google Guava Iterables
        // pagination to avoid DynamoDB Validation Exceptions
        Iterable<List<String>> documentList = Iterables.partition(documents, 100);

        // code from https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-document-api-java.html
        for (List<String> docList: documentList) {
            TableKeysAndAttributes forumTableKeysAndAttributes = new TableKeysAndAttributes(pagerankTableName);
            // TODO: double check with PageRankDB of the name
            forumTableKeysAndAttributes.withAttributeNames("url", "score");

            for (String docID: docList) {
                forumTableKeysAndAttributes.addHashOnlyPrimaryKeys("url", docID);
            }
            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(forumTableKeysAndAttributes);

            Map<String, KeysAndAttributes> unprocessed = null;

            do {
                List<Item> items = outcome.getTableItems().get(pagerankTableName);
                for (Item item: items) {
                    String finalDocID = (String) item.getString("url");
                    Double finalPagerank = Double.valueOf((String) item.get("score"));
                    logger.info("pagerank: " + count + "======" + documents.size());
                    count++;
                    pagerankScore.put(finalDocID, finalPagerank);
                }

                unprocessed = outcome.getUnprocessedKeys();
            } while (!unprocessed.isEmpty());
        } 

        logger.info("finish getting pagerank using batchGet");
        return pagerankScore;
    }

    public Map<String, Item> getDocumentBatch(Set<String> documents) {
        logger.info("starting getting document using batchGet");
        int count = 0;
        Map<String, Item> documentItem = new HashMap<String, Item>();
        String documentTableName = this.documentTable.getTableName();

        // Google Guava Iterables
        // pagination to avoid DynamoDB Validation Exceptions
        Iterable<List<String>> documentList = Iterables.partition(documents, 100);

        // code from https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-document-api-java.html
        for (List<String> docList: documentList) {
            TableKeysAndAttributes forumTableKeysAndAttributes = new TableKeysAndAttributes(documentTableName);
            // TODO: double check with PageRankDB of the name
            forumTableKeysAndAttributes.withAttributeNames("url", "title");

            for (String docID: docList) {
                forumTableKeysAndAttributes.addHashOnlyPrimaryKeys("url", docID);
            }
            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(forumTableKeysAndAttributes);

            Map<String, KeysAndAttributes> unprocessed = null;

            do {
                List<Item> items = outcome.getTableItems().get(documentTableName);
                for (Item item: items) {
                    String finalDocID = (String) item.getString("url");
                    // logger.info("url: " + finalDocID + " score: " + finalPagerank);
                    documentItem.put(finalDocID, item);
                    logger.info("document: " + count + "======" + documents.size());
                    count++;
                }

                unprocessed = outcome.getUnprocessedKeys();
            } while (!unprocessed.isEmpty());
        } 

        logger.info("finish getting document using batchGet");
        return documentItem;
    }

    public Map<String, Double> getQueryTermFreq(List<String> termList) {
        Map<String, Double> queryTermFreq = new HashMap<String, Double>();
        
        // update query term weight map
        for (String term: termList) {
            if (queryTermFreq.containsKey(term)) {
                queryTermFreq.put(term, queryTermFreq.get(term) + 1.0);
            } else {
                queryTermFreq.put(term, 1.0);
            }
        }

        for (String term: queryTermFreq.keySet()) {
            double count = queryTermFreq.get(term);
            queryTermFreq.put(term, count);
        }

        return queryTermFreq;
    }

}
