package edu.upenn.cis.stormlite;

import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.DocFetcherBolt;
import edu.upenn.cis.stormlite.bolt.FilterBolt;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.LinkExtractBolt;
import edu.upenn.cis.stormlite.bolt.WriterBolt;
import edu.upenn.cis.stormlite.spout.CrawlerQueue;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.tuple.Fields;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Simple word counter test case, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class CrawlerStorm {
    static Logger log = LogManager.getLogger(CrawlerStorm.class);
    public static Config config;
    public static CountDownLatch doneSignal;
    public static LocalCluster cluster;
    public static TopologyBuilder builder;
    private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
    private static final String URL_FILTER_BOLT = "URL_FILTER_BOLT";
    private static final String DOC_FETCH_BOLT = "DOC_FETCH_BOLT";
    private static final String LINK_EXTRACT_BOLT = "LINK_EXTRACT_BOLT";
    private static final String WRITER_BOLT = "WRITER_BOLT";

    public void init() throws Exception {
        config = new Config();
        CrawlerQueue queueSpout = new CrawlerQueue();
        FilterBolt filter = new FilterBolt();
        DocFetcherBolt fetcher = new DocFetcherBolt();
        LinkExtractBolt extracter = new LinkExtractBolt();
        WriterBolt writer = new WriterBolt();

        // CrawlerQueue ==> URLFilter ===> DocFetcher ==> LinkExtract ==> Writer
        builder = new TopologyBuilder();

        // Only one source ("spout") for crawler queue
        builder.setSpout(QUEUE_SPOUT, queueSpout, 5);

        // parallel url filters, each of which gets the same kinds of urls
        builder.setBolt(URL_FILTER_BOLT, filter, 15).fieldsGrouping(QUEUE_SPOUT, new Fields("Root"));

        // parallel doc fetchers, each of which gets the same kinds of urls
        builder.setBolt(DOC_FETCH_BOLT, fetcher, 20).fieldsGrouping(URL_FILTER_BOLT, new Fields("Root"));

        // parallel link extractors - round-robin
        builder.setBolt(LINK_EXTRACT_BOLT, extracter, 10).fieldsGrouping(DOC_FETCH_BOLT, new Fields("URL"));

        // Single DB writer
        builder.setBolt(WRITER_BOLT, writer, 5).shuffleGrouping(LINK_EXTRACT_BOLT);

        CrawlerStorm.cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
        try {
            String str = mapper.writeValueAsString(topo);
            System.out.println("The StormLite topology is:\n" + str);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void run() {
        cluster.submitTopology("test", config, builder.createTopology());
    }

    public void stop() {
        CrawlerQueue.stop();
    }

    public void shutdown() {
        System.out.println("try shutting down the storm");
        try {
            setLatches();
            toCountDown();
            doneSignal.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("All executors have exited");
        cluster.killTopology("test");
        CrawlerStorm.cluster.shutdown();
    }

    public static void setLatches() {
        int sCount = cluster.spoutStreams.size(), bCount = cluster.boltStreams.size();
        doneSignal = new CountDownLatch(sCount + bCount);
        for (String key : cluster.spoutStreams.keySet()) {
            for (IRichSpout spout : cluster.spoutStreams.get(key)) {
                ((CrawlerQueue) spout).setLatch(doneSignal);
            }
        }
        for (String key : cluster.boltStreams.keySet()) {
            for (IRichBolt bolt : cluster.boltStreams.get(key)) {
                if (bolt instanceof FilterBolt) {
                    ((FilterBolt) bolt).setLatch(doneSignal);
                }
                if (bolt instanceof DocFetcherBolt) {
                    ((DocFetcherBolt) bolt).setLatch(doneSignal);
                }
                if (bolt instanceof LinkExtractBolt) {
                    ((LinkExtractBolt) bolt).setLatch(doneSignal);
                }
                if (bolt instanceof WriterBolt) {
                    ((WriterBolt) bolt).setLatch(doneSignal);
                }
            }
        }
    }

    public static void toCountDown() {
        for (String key : cluster.spoutStreams.keySet()) {
            for (IRichSpout spout : cluster.spoutStreams.get(key)) {
                ((CrawlerQueue) spout).toCountDown();
            }
        }
        for (String key : cluster.boltStreams.keySet()) {
            for (IRichBolt bolt : cluster.boltStreams.get(key)) {
                if (bolt instanceof FilterBolt) {
                    System.out.println("start counting down a FilterBolt");
                    ((FilterBolt) bolt).toCountDown();
                }
                if (bolt instanceof DocFetcherBolt) {
                    System.out.println("start counting down a DocFetcherBolt");
                    ((DocFetcherBolt) bolt).toCountDown();
                }
                if (bolt instanceof LinkExtractBolt) {
                    System.out.println("Now processing a LinkExtractBolt");
                    ((LinkExtractBolt) bolt).toCountDown();
                }
                if (bolt instanceof WriterBolt) {
                    System.out.println("Now processing a WriterBolt");
                    ((WriterBolt) bolt).toCountDown();
                }

            }
        }
    }

}
