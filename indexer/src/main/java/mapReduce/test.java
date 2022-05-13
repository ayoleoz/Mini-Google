package mapReduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.upenn.cis.cis455.AWS.InvertedIndex;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<InvertedIndex> indexLst = new ArrayList<InvertedIndex>();
		InvertedIndex record = new InvertedIndex();
		record.setWord("a");
		record.setUrl("b");
		record.setTfidf(Double.toString(1.2));
		
		
		indexLst.add(record);
		
		record = new InvertedIndex();
		record.setWord("c");
		record.setUrl("c");
		record.setTfidf(Double.toString(0.8));
		
		indexLst.add(record);
		
		record = new InvertedIndex();
		record.setWord("d");
		record.setUrl("d");
		record.setTfidf(Double.toString(0.5));
		
		indexLst.add(record);
		
		
		Collections.sort(indexLst, new Comparator<InvertedIndex>(){
		     public int compare(InvertedIndex o1, InvertedIndex o2){
		         if(o1.tfidf == o2.tfidf)
		             return 0;
		         return Double.parseDouble(o1.tfidf) < Double.parseDouble(o2.tfidf) ? -1 : 1;
		     }
		});
		System.out.println(indexLst.get(0).tfidf);
		System.out.println(indexLst.get(1).tfidf);
		System.out.println(indexLst.get(2).tfidf);
	}

}
