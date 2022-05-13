package edu.upenn.cis.cis455.crawler;

import java.util.Iterator;
import java.util.TreeSet;

public class Rule {
    public long lastCrawl;
    public long crawDelay = 5;
    public boolean ruleAdded = false;
    public TreeSet<String> notAllowed;
    public TreeSet<String> allowed;

    public Rule() {
        notAllowed = new TreeSet<>();
        allowed = new TreeSet<>();
        lastCrawl = new java.util.Date().getTime();
    }

    public boolean subCat(String rule, String path) {
        if (rule.isBlank() || !rule.contains("/")) {
            return false;
        }
        if (path.isBlank() || !path.contains("/")) {
            return true;
        }
        int rI = 0, pI = 0;
        String[] ruleSplit = rule.split("/");
        String[] pathSplit = path.split("/");
        while (rI < ruleSplit.length && pI < pathSplit.length) {
            String rCurr = ruleSplit[rI];
            String pCurr = pathSplit[pI];
            if (!rCurr.equals(pCurr)) {
                return false;
            }
            rI++;
            pI++;
        }
        return rI >= ruleSplit.length;
    }

    public boolean canCrawl(String pathInfo) {
        if (!ruleAdded) {
            return false;
        }
        Iterator<String> itr = notAllowed.iterator();
        boolean prevented = false;
        while (itr.hasNext()) {
            String disallow = itr.next();
            if (pathInfo.startsWith(disallow)) {
                if (pathInfo.equals(disallow)) {
                    return false;
                }
                if (subCat(disallow, pathInfo)) {
                    prevented = true;
                    break;
                }
            }
        }
        if (!prevented) {
            return true;
        }
        Iterator<String> itrAllow = allowed.iterator();
        while (itrAllow.hasNext()) {
            String allow = itrAllow.next();
            if (pathInfo.startsWith(allow)) {
                return true;
            }
            if (subCat(allow, pathInfo)) {
                return true;
            }
        }
        return false;
    }

    public void resetRule() {
        allowed.clear();
        notAllowed.clear();
        crawDelay = 5;
    }

    public void setRule(String s) {
        if (!s.startsWith("Allow") && !s.startsWith("Disallow") && !s.startsWith("Crawl-delay") && !s.contains(":")) {
            return;
        }
        String[] split = s.split(":");
        if (split.length <= 1) {
            return;
        }
        String r = split[1].trim();
        if (s.startsWith("Allow")) {
            allowed.add(r);
        } else if (s.startsWith("Disallow")) {
            notAllowed.add(r);
        } else if (s.startsWith("Crawl-delay")) {
            crawDelay = Long.parseLong(r);
        }
    }

    public synchronized void updateLastCrawl() {
        lastCrawl = new java.util.Date().getTime();
    }

    public boolean shouldSkip() {
        if (crawDelay < 0) {
            return false;
        }
        long currTime = new java.util.Date().getTime();
        return lastCrawl + crawDelay * 1000 > currTime;
    }

}
