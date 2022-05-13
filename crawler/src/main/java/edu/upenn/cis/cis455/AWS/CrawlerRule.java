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
public class CrawlerRule {

    private String url;
    private String rule;

    @DynamoDbPartitionKey
    public String getUrl() {
        return this.url;
    };

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRule() {
        return this.rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }
}
