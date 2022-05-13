package edu.upenn.cis.cis455.crawler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class CrawlerData {
    private String lastChecked;

    public CrawlerData() {
        updateLastChecked();
    }

    public String getLastChecked() {
        return lastChecked;
    }

    private void setChecked(String time) {
        lastChecked = time;
    }

    private String getDate(Date date) {
        // Calendar gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat formatter = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        String strDate = formatter.format(date);
        return strDate;
    }

    private void updateLastChecked() {
        Date date = new Date();
        setChecked(getDate(date));
    }

}
