package edu.upenn.cis.cis455.crawler.utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunc {
    public static String MD5(String content) {
        if (content == null) {
            return "";
        }
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
            md5.update(StandardCharsets.UTF_8.encode(content));
            return String.format("%032x", new BigInteger(1, md5.digest()));

            // byte[] digest = md5.digest();
            // StringBuffer sb = new StringBuffer();
            // for (int i = 0; i < digest.length; ++i) {
            // sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100).substring(1, 3));
            // }
            // return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "";
        }
    }

    public static String SHA256(String pwd) {
        if (pwd == null) {
            return "";
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(pwd.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(2 * hash.length);
            for (int i = 0; i < hash.length; i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
    }
}
