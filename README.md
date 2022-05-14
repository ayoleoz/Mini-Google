# Mini-Google

A distributed Web Indexer and Crawler with User Interface

The project is divided up into several parts:

1. Crawler, which parses typical HTML documents. It checks for and respect the restrictions in robots.txt. The requests are distributed, Mercator-style, and across multiple crawling peers (**distributed StormLite implementation**).

2. Indexer, which takes words and other information from the crawler and creates a lexicon, inverted index. It uses MapReduce to generate the data structures from the output of the crawler and stores the resulting index data on DynamoDB.

3. PageRank, which performs link analysis using the PageRank algorithm and is expected to support detecting dangling links and self-loops.

4. Search Engine and Web UI, which provides a search form and weighted results list by querying from the DynamoDB and displaying the query results on a Web User Interface.

The detailed README is in each sub-directory with instructions to run the code.
