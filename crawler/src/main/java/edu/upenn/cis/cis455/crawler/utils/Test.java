package edu.upenn.cis.cis455.crawler.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.jsoup.nodes.Document;

import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.crawler.Info;
import edu.upenn.cis.cis455.crawler.Rule;
import edu.upenn.cis.cis455.crawler.master.MasterHelper;

public class Test {
    public static void processRecord(String url) {
        URL url1;
        try {
            url1 = new URL(url);
            byte[] ba1 = new byte[1024];
            int baLength;
            FileOutputStream fos1 = new FileOutputStream("download.pdf");
            URLConnection urlConn = url1.openConnection();
            // Checking whether the URL contains a PDF
            if (!urlConn.getContentType().equalsIgnoreCase("application/pdf")) {
                System.out.println("FAILED.\n[Sorry. This is not a PDF.]");
            } else {
                try {
                    // Read the PDF from the URL and save to a local file
                    InputStream is1 = url1.openStream();
                    while ((baLength = is1.read(ba1)) != -1) {
                        fos1.write(ba1, 0, baLength);
                    }
                    fos1.flush();
                    fos1.close();
                    is1.close();
                } catch (IOException ce) {
                    System.out.println("FAILED.\n[" + ce.getMessage() + "]\n");
                }
            }
        } catch (NullPointerException | IOException npe) {
            // System.out.println("FAILED.\n[" + npe.getMessage() + "]\n");
        }
        File file = new File("download.pdf");
        PDDocument document;
        try {
            document = PDDocument.load(file);
            PDFTextStripper pdfStripper = new PDFTextStripper();
            String text = pdfStripper.getText(document);
            System.out.println(text);
            document.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static void hashIntoBucket() {
        String wiki = "https://en.wikipedia.org/wiki/Main_Page";
        String yahoo = "https://www.mayoclinic.org/";

        URLInfo wikiInfo = new URLInfo(wiki);
        URLInfo yhInfo = new URLInfo(yahoo);
        System.out.println(wikiInfo.getHostName().hashCode() % 2);
        System.out.println(yhInfo.getHostName().hashCode() % 2);

    }

    public static Rule robotInfo() {
        String wiki = "https://elnacional.com.do/robots.txt";
        String root = new URLInfo(wiki).uri();
        Rule rule = Parser.consultRobot(root);
        return rule;
    }

    public static Info headInfo() {
        String wiki = "https://olympics.com/en/athletes/eva-samkova";
        URLInfo root = new URLInfo(wiki);
        Info info = Parser.fetchInfoByHead(wiki, root);
        return info;
    }

    public static void hash(String url) {
        URLInfo info = new URLInfo(url);
        String host = info.getHostName();
        int bucket = Math.abs(host.hashCode()) % 5;
        System.out.println(host + ": " + bucket);
    }

    public static void main(String[] args) {
        // processRecord("https://www.cmu.edu/teaching/designteach/syllabus/samples/Birth%20of%20Modern%20Childbirth%20Syllabus_FINAL.pdf");
        // String wiki = "https://olympics.com/en/athletes/eva-samkova";
        // URLInfo root = new URLInfo(wiki);
        Long time = System.currentTimeMillis();
        String date = MasterHelper.format(time);
        System.out.println(date);
        // hash("https://www.apple.com/?afid=p238%7CseIEs444j-dc_mtid_1870765e38482_pcrid_592988481760_pgrid_13945964887_pntwk_g_pchan__pexid__&cid=aos-us-kwgo-brand-apple--slid---product-");
    }
}
