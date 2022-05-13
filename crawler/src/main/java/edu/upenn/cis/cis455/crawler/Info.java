package edu.upenn.cis.cis455.crawler;

public class Info {
    public int status;
    public String contentType;
    public double fileSize;

    public void setStatus(int s) {
        this.status = s;
    }

    public void setContentType(String s) {
        this.contentType = s;
    }

    public void setFileSize(double size) {
        this.fileSize = size;
    }

    public int status() {
        return this.status;
    }

    public String contentType() {
        return this.contentType;
    }

    public double fileSize() {
        return this.fileSize;
    }
}
