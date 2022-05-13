package edu.upenn.cis.cis455.crawler.master;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.AWS.SQS;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import software.amazon.awssdk.services.sqs.model.Message;

public class LoadBalancer {
    final static Logger logger = LogManager.getLogger(LoadBalancer.class);

    public void sendOtherWork() {
        List<Message> messages = SQS.receiveMessages(SQS.QUEUE_TWO_ADDR);
        for (Message m : messages) {
            String url = m.body();
            if (skipNonWikiPage(url)) {
                logger.info("not an interesting page -- skip");
                continue;
            }
            URLInfo info = new URLInfo(url);
            String host = info.getHostName();
            int bucket = Math.abs(host.hashCode()) % MasterServer.workers.size();
            String worker = MasterServer.workers.get(bucket);
            URL toSend;
            String addr = "http://" + worker + "/getWork?url=" + url;
            logger.info("send " + url + "to worker " + bucket);
            try {
                toSend = new URL(addr);
                HttpURLConnection conn = (HttpURLConnection) toSend.openConnection();
                conn.setRequestMethod("GET");
                int responseCode = conn.getResponseCode();
                logger.info("response code: " + responseCode);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SQS.deleteMessages(messages, SQS.QUEUE_TWO_ADDR);
    }

    public void sendWikiWork() {
        List<Message> wikiMessages = SQS.receiveMessages(SQS.QUEUE_ADDR);
        for (Message m : wikiMessages) {
            String url = m.body();
            if (skipWikiPage(url)) {
                logger.info("not an english wiki page with interesting info -- skip");
                continue;
            }
            URLInfo info = new URLInfo(url);
            String host = info.getHostName();
            int bucket = Math.abs(host.hashCode()) % MasterServer.workers.size();
            String worker = MasterServer.workers.get(bucket);
            URL toSend;
            String addr = "http://" + worker + "/getWork?url=" + url;
            logger.info("send " + url + "to worker " + bucket);
            try {
                toSend = new URL(addr);
                HttpURLConnection conn = (HttpURLConnection) toSend.openConnection();
                conn.setRequestMethod("GET");
                int responseCode = conn.getResponseCode();
                logger.info("response code: " + responseCode);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }
        SQS.deleteMessages(wikiMessages, SQS.QUEUE_ADDR);
    }

    public boolean skipWikiPage(String url) {
        if (!url.contains("wiki") || url.endsWith("jpg") || url.endsWith("gif") || url.contains("search")
                || url.contains("google") || !url.contains("en.wikipedia.org")
                || (url.contains("#") || url.contains("user") || url.contains("special"))) {
            return true;
        }
        return false;
    }

    public boolean skipNonWikiPage(String url) {
        if (url.endsWith("jpg") || url.endsWith("gif") || url.contains("search") || url.contains("google")
                || url.contains("wiki") || url.contains("gov") || url.contains("video") || url.contains("login")
                || url.contains("account") || url.contains("liscence") || url.contains("cookie")) {
            return true;
        }
        return false;
    }

}
