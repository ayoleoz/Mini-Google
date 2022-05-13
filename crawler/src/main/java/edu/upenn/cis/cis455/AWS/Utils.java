package edu.upenn.cis.cis455.AWS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;

public class Utils {
    public static AwsBasicCredentials awsCreds = AwsBasicCredentials.create("AKIA25X74WTVTSJ7D5DT",
            "NIq9KzJ6BWJfsN5y60Isxflc77JHBOt4N8CPvg2S");
    public static Region region = Region.US_EAST_1;

    /**
     * split sets into list of sets
     */
    public static <T> List<ArrayList<T>> split(List<T> original, int each) {
        List<ArrayList<T>> result = new ArrayList<ArrayList<T>>();
        Iterator<T> it = original.iterator();
        int count = original.size() / each;
        if (original.size() % each != 0) {
            count++;
        }
        for (int i = 0; i < count; i++) {
            ArrayList<T> s = new ArrayList<T>();
            result.add(s);
            for (int j = 0; j < each && it.hasNext(); j++) {
                s.add(it.next());
            }
        }
        return result;
    }

    /**
     * split sets into list of sets
     */
    public static <T> List<Set<T>> split(Set<T> original, int each) {
        ArrayList<Set<T>> result = new ArrayList<Set<T>>();
        Iterator<T> it = original.iterator();
        int count = original.size() / each;
        if (original.size() % each != 0) {
            count++;
        }
        for (int i = 0; i < count; i++) {
            HashSet<T> s = new HashSet<T>();
            result.add(s);
            for (int j = 0; j < each && it.hasNext(); j++) {
                s.add(it.next());
            }
        }
        return result;
    }
}
