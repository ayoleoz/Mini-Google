/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

package edu.upenn.cis.cis455.AWS;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * This class is used by the Enhanced Client examples.
 */

@DynamoDbBean
public class WebLink {
    private String from;
    private String to;

    @DynamoDbPartitionKey
    public String getFrom() {
        return this.from;
    };

    public void setFrom(String url) {
        this.from = url;
    }

    @DynamoDbSortKey
    public String getTo() {
        return this.to;
    }

    public void setTo(String url) {
        this.to = url;
    }
}
