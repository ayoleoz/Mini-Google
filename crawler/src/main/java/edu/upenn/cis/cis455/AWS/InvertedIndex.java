/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/


package edu.upenn.cis.cis455.AWS;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * This class is used by the Enhanced Client examples.
 */

@DynamoDbBean
public class InvertedIndex {

    public String word;
    public String url;
    public String tfidf;

    @DynamoDbPartitionKey
    public String getWord() {
        return this.word;
    };

    public void setWord(String word) {
        this.word = word;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    public String getTfidf() {
        return this.tfidf;
    }

    public void setTfidf(String tfidf) {
        this.tfidf = tfidf;
    }
}
