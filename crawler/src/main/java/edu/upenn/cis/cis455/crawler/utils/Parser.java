package edu.upenn.cis.cis455.crawler.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.net.ssl.HttpsURLConnection;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.cis455.crawler.Info;
import edu.upenn.cis.cis455.crawler.Rule;

public class Parser {
    public static Rule consultRobot(String root) {
        Rule toVisit = new Rule();
        String source = root + "/robots.txt";
        try {
            URL url = new URL(source);
            URLConnection con = url.openConnection();
            con.setConnectTimeout(10000);
            con.setReadTimeout(10000);
            InputStream in = con.getInputStream();
            Scanner s = new Scanner(in);
            String line = "";
            boolean previous = false, exact = false, general = false;
            while (s.hasNextLine() && !exact) {
                if (!previous) {
                    line = s.nextLine();
                }
                // System.out.println(line);
                String agent = "";
                if (line.startsWith("User-agent") && !exact) {
                    agent = line.split(":")[1].trim();
                    if (previous) {
                        previous = false;
                    }
                    if (agent.equals("cis455crawler")) {
                        exact = true;
                    } else if (agent.equals("*")) {
                        general = true;
                    } else {
                        continue;
                    }
                    while (s.hasNextLine()) {
                        line = s.nextLine();
                        if (!line.startsWith("User-agent") && (line.startsWith("Disallow")
                                || line.startsWith("Crawl-delay") || line.startsWith("Allow"))) {
                            if (exact || general) {
                                if (exact) {
                                    if (general) {
                                        toVisit.resetRule();
                                        general = false;
                                    }
                                }
                                toVisit.setRule(line);
                            }
                        } else if (line.startsWith("User-agent")) {
                            previous = true;
                            break;
                        } else {
                            if (line.isEmpty()) {
                                continue;
                            }
                            if (!line.contains("cis455crawler")) {
                                break;
                            }
                        }
                    }
                }

            }
            s.close();
        } catch (IOException e) {
            e.printStackTrace();
            // It's fine if IOException is thrown
            toVisit.ruleAdded = false;
            return toVisit;
        }
        toVisit.ruleAdded = true;
        toVisit.updateLastCrawl();
        return toVisit;
    }

    public static Document fetchContent(String source) {
        try {
            Document doc = Jsoup.connect(source).userAgent("cis455crawler").timeout(10000).get();
            return doc;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static Info fetchInfoByHead(String source, URLInfo info) {
        System.out.println("check if can fetch by sending a head request to " + source);
        String contentType = null;
        long fileSize = -1;
        int status = 0;
        if (info.isSecure()) {
            try {
                HttpsURLConnection url = (HttpsURLConnection) new URL(source).openConnection();
                url.setRequestMethod("HEAD");
                url.setConnectTimeout(5000);
                url.setReadTimeout(5000);
                url.addRequestProperty("Host", info.getHostName());
                url.addRequestProperty("User-Agent", "cis455crawler");
                url.connect();
                status = url.getResponseCode();
                contentType = url.getContentType();
                fileSize = url.getContentLengthLong();
                // url.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
                Info headResponse = new Info();
                headResponse.setStatus(400);
                return headResponse;
            }
        } else {
            Info headResponse = new Info();
            headResponse.setStatus(400);
            return headResponse;

        }
        Info headResponse = new Info();
        headResponse.setStatus(status);
        headResponse.setContentType(contentType);
        headResponse.setFileSize(fileSize / 1048576.0);
        return headResponse;
    }

    public static boolean canFetch(Info head, int size) {
        System.out.println("check if can fetch by verifying the type and size");
        if (head == null) {
            return false;
        }
        // System.out.println("status: " + head.status);
        // System.out.println("content type: " + head.contentType);
        // System.out.println("file size: " + head.fileSize);
        double fileSize = head.fileSize;
        String contentType = head.contentType;
        int status = head.status;
        if (status > 0) {
            if (status != 200) {
                System.out.println("error code");
                return false;
            }
        }
        if (contentType != null) {
            if (contentType.contains("text/html") || contentType.contains("text/xml")
                    || contentType.contains("application/xml") || contentType.contains("+xml")
            // || contentType.equalsIgnoreCase("application/pdf")
            ) {
                if (fileSize >= 0) {
                    if (fileSize > size) {
                        System.out.println("File is too big to fetch");
                        return false;
                    }
                    return true;
                }
                System.out.println("Don't know the content length");
                return true;
            } else {
                System.out.println("Don't have the content type of interest");
                return false;
            }
        }
        System.out.println("Unknown content type");
        return false;
    }

    public static List<Set<String>> extractLinks(String html, String baseUri) {
        List<Set<String>> result = new ArrayList<>();
        Set<String> wikiURLs = new HashSet<>();
        Set<String> otherURLs = new HashSet<>();
        if (html == null) {
            return result;
        }
        Document doc = Jsoup.parse(html);
        Elements links = doc.select("a[href]");
        for (Element r : links) {
            r.setBaseUri(baseUri);
            String newLink = r.absUrl("href");
            if (newLink.isEmpty()) {
                newLink = r.baseUri() + r.attr("href");
            }
            // System.out.println("Link: " + newLink);
            if (newLink.isEmpty() || newLink.length() > 180) {
                System.out.println("URL is too long, skip to filter out potential low-quality content");
                continue;
            } else if (newLink.endsWith("jpg") || newLink.endsWith("gif") || newLink.contains("google")
                    || newLink.contains("spam") || newLink.contains("gov") || newLink.contains("doi")
                    || newLink.contains("liscence") || newLink.contains("cookie")
                    || (newLink.contains("wiki") && !newLink.contains("en.wikipedia.org"))) {
                continue;
            } else {
                if (newLink.contains("en.wikipedia.org") && !newLink.contains("user") && !newLink.contains("#")
                        && !newLink.contains("ISBN")) {
                    wikiURLs.add(newLink);
                } else if (!newLink.contains("wiki")) {
                    otherURLs.add(newLink);
                }
            }
        }
        result.add(wikiURLs);
        result.add(otherURLs);
        return result;
    }

    public static boolean fetchPDF(String source, String savePath) {
        URL url;
        try {
            url = new URL(source);
            byte[] buffer = new byte[1024];
            int length;
            FileOutputStream fos = new FileOutputStream(savePath);
            URLConnection urlConn = url.openConnection();
            urlConn.setConnectTimeout(10000);
            // verify again the URL contains a PDF
            if (!urlConn.getContentType().equalsIgnoreCase("application/pdf")) {
                System.out.println("FAILED.\n[Sorry. This is not a PDF.]");
            } else {
                try {
                    // Read the PDF from the URL and save to a local file
                    InputStream input = url.openStream();
                    while ((length = input.read(buffer)) != -1) {
                        fos.write(buffer, 0, length);
                    }
                    fos.flush();
                    fos.close();
                    input.close();
                    return true;
                } catch (IOException ce) {
                    System.out.println("FAILED.\n[" + ce.getMessage() + "]\n");
                }
            }
        } catch (NullPointerException | IOException npe) {
            System.out.println("FAILED.\n[" + npe.getMessage() + "]\n");
        }
        return false;
    }

    public String extractTextFromPDF(String filePath) {
        PDDocument document;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                return null;
            }
            document = PDDocument.load(file);
            PDFTextStripper pdfStripper = new PDFTextStripper();
            String text = pdfStripper.getText(document);
            document.close();
            return text;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static boolean zip(String sourceFile, String fileName, String outputPath) {
        try {
            FileOutputStream fos = new FileOutputStream(outputPath);
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            File fileToZip = new File(sourceFile);
            zipFile(fileToZip, fileName, zipOut);
            zipOut.close();
            fos.close();
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) {
        try {
            if (!fileToZip.exists() || fileToZip.isHidden()) {
                return;
            }
            if (fileToZip.isDirectory()) {
                if (fileName.endsWith("/")) {
                    zipOut.putNextEntry(new ZipEntry(fileName));

                    zipOut.closeEntry();
                } else {
                    zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                    zipOut.closeEntry();
                }
                File[] children = fileToZip.listFiles();
                for (File childFile : children) {
                    zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
                }
                return;
            }
            FileInputStream fis = new FileInputStream(fileToZip);
            ZipEntry zipEntry = new ZipEntry(fileName);
            zipOut.putNextEntry(zipEntry);
            byte[] bytes = new byte[1024];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fis.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
