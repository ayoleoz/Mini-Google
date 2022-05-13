package edu.upenn.cis.cis455.AWS;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class VisitedPage {

    private String url;
    private String crawledTime;
    private String preview;
    private String S3Key;
    private String title;
    private long byteRangeStart; // inclusive
    private long byteRangeEnd; // until, but doesn't include

    @DynamoDbPartitionKey
    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPreview() {
        return this.preview;
    }

    public void setpreview(String pre) {
        this.preview = pre;
    }

    public String getS3Key() {
        return this.S3Key;
    }

    public void setS3Key(String key) {
        this.S3Key = key;
    }

    public String getCrawledTime() {
        return this.crawledTime;
    }

    public void setCrawledTime(String time) {
        this.crawledTime = time;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getByteRangeStart() {
        return this.byteRangeStart;
    }

    public void setByteRangeStart(long start) {
        this.byteRangeStart = start;
    }

    public long getByteRangeEnd() {
        return this.byteRangeEnd;
    }

    public void setByteRangeEnd(long end) {
        this.byteRangeEnd = end;
    }
}