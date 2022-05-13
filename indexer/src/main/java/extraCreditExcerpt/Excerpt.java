package extraCreditExcerpt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Excerpt {

	private String word;
	private String context;
	private String url;

	private String getHTML(String url) throws IOException {
		Document doc = Jsoup.connect(url).get();
		String content = doc.text();
		return content;
	}

	public String getExcerptForWord(String word, String url) throws IOException {

		String content = getHTML(url);
		List<String> strLst = Arrays.asList(content.split(" "));
		
		if(strLst.contains(word)) {
			int target = strLst.indexOf(word);
			List<String> ret = strLst.subList(Math.max(0, target-5), Math.min(strLst.size(), target+5));
			String joined = String.join(" ", ret);
			return joined;
		}
		return "";
	}

}
