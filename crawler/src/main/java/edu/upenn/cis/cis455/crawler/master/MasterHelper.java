package edu.upenn.cis.cis455.crawler.master;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Date;
import java.text.SimpleDateFormat;

public class MasterHelper {
    public static void sendGetRequest(String dest, String job) {
        String addr = "http://" + dest + "/" + job;
        try {
            URL url = new URL(addr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            int responseCode = conn.getResponseCode();
            System.out.println("response code: " + responseCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isActive(long lastReport) {
        long currTime = System.currentTimeMillis();
        return currTime - lastReport <= 120000;
    }

    public static void setActiveWorkers(String reporter, String indexed) {
        MasterServer.workerStatus.put(reporter, System.currentTimeMillis());
        MasterServer.progress.put(reporter, indexed);
        if (!MasterServer.workers.contains(reporter)) {
            MasterServer.workers.add(reporter);
        }
        // remove inactive workers
        for (String worker : MasterServer.workerStatus.keySet()) {
            if (!isActive(MasterServer.workerStatus.get(worker))) {
                MasterServer.workerStatus.remove(worker);
                MasterServer.progress.remove(worker);
                if (MasterServer.workers.contains(worker)) {
                    MasterServer.workers.remove(worker);
                }
            }
        }
        System.out.println("Current active workers: " + MasterServer.workers.toString());
    }

    public static String format(Long time) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a");
        Date date = new Date(time);
        String t = simpleDateFormat.format(date);
        return t;
    }

}
