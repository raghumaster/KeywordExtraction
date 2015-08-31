package org.csulb.edu.keywordextraction.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//Java class for identifying a posting
public class Posting {
	private long id;
	private String title;
	private String body;
	private List<String> tags;
	private String codeSection;
	private Map<String,Integer> tokens;
	private  Set<String> stopWords ;
		
	/** Parameterized Constructor for initializing the posting object
	 * If isTraining is true -> training data set
	 * Else -> test data set (for test data set we don't have tags)
	 * @param record Record retrieved by the Mapper
	 * @param stopWords hash set containing all the stop words
	 */
	public Posting(String record, Set<String> stopWords, boolean isTraining) {
		this.stopWords = stopWords;
		tokens = new HashMap<>();
		String[] input = record.toString().split(",");
		int n = input.length;
		this.id = Long.parseLong(input[0]);
		this.title = cleanString(input[1]);
		StringBuffer body = new StringBuffer();
		
		//For training data the last column is of the csv file is tags
		if(isTraining){
			for(int i=2;i<n-1;i++){
				body.append(input[i]);
			}
		}else{
			//Test data doesn't contain the tags column
			for(int i=2;i<n;i++){
				body.append(input[i]);
			}
		}
		StringBuffer codeSection = new StringBuffer();
		String cleanedBody;
		int startIndex,endIndex,loopCount,counter=0;
        //Code for extracting code section
        while(body.toString().contains("<code>")){
                startIndex = body.indexOf("<code>");
                endIndex = body.indexOf("</code>");
                loopCount=endIndex+6-startIndex;
                counter=0;
                while(counter<=loopCount){
                        codeSection.append(body.charAt(startIndex));
                        body.deleteCharAt(startIndex);
                        counter++;
                }	
        }
		if(codeSection.length()==0)
			this.codeSection = null;
		else
			this.codeSection = codeSection.toString();
		
		this.body = cleanString(body.toString());
		//Tokenization of the question title and body
		addToTokensMap(this.title);
		addToTokensMap(this.body);
		//Add tags to an array list if the input data set is training data set
		if(isTraining){
			String[] tags = input[n-1].split(" ");
			this.tags = new ArrayList<>();
			for(String tag:tags){
				this.tags.add(tag);
			}
		}else{
			//For test data there will not be any tags
			this.tags = null;
		}
	}
	
	
	/**
	 *  Tokenization of the given input using space as delimeter and adding the tokens to a hash map along with frequency
	 *  @param input, the input string to be tokenized
	 */
	public void addToTokensMap(String input) {
		//Add the cleaned body to posting
		String[] splitTokens = input.split(" ");
		String currentToken;
		int count;
		for (int i=0;i<splitTokens.length;i++) {
			currentToken = splitTokens[i];
			if (!stopWords.contains(currentToken)) {
				if (tokens.containsKey(currentToken)) {
					count = tokens.get(currentToken);
					tokens.put(currentToken, count+1);
				}else {
					tokens.put(currentToken,1);
				}
			}
		}
	}
	
	/**
	 * Method for performing the cleaning (or remove noises)
	 * @param input, the input string to be cleaned 
	 */
	public static String cleanString(String input){
		String cleanedStr;
		//Convert to lowercase, 
		cleanedStr = input.toLowerCase();
		//Remove all html tags 
		cleanedStr = cleanedStr.replaceAll("\\<.*?>","");
		//Remove special characters from body
		cleanedStr = cleanedStr.replaceAll("[\\-\\\"\\+\\^:,()?*]"," ");
		cleanedStr = cleanedStr.replaceAll("[\\.']","");
		//Trim additional white spaces
		cleanedStr = cleanedStr.trim();
		return cleanedStr;
	}
	
	//Getters and setters
	
	public String toString(){
		//return id+": "+title+"\n"+body+"\n"+codeSection+"\n"+tags;
		return id+":"+tokens+"\n"+tags;
	}

	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getCodeSection() {
		return codeSection;
	}

	public void setCodeSection(String codeSection) {
		this.codeSection = codeSection;
	}

	public Map<String,Integer> getTokens() {
		return tokens;
	}

	public void setTokens(Map<String,Integer> tokens) {
		this.tokens = tokens;
	}	
}
