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
	private static String fileNames[];
	private  Set<String> stopWords ;
	
	
	//Parameterized Constructor
	/*
	 * If isTraining is true -> training data set
	 * Else -> test data set (for test data set we don't have tags)
	 */
	public Posting(String record,Set<String> stop,boolean isTraining){
		stopWords = stop;
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
	
	
	/*
	 *  Tokenization the given question using space as delimeter
	 */
	public void addToTokensMap(String str){
		//Add the cleaned body to posting
		String[] splitTokens = str.split(" ");
		String currentToken;
		int count;
		for(int i=0;i<splitTokens.length;i++){
			currentToken = splitTokens[i];
			if(!stopWords.contains(currentToken)){
				if(tokens.containsKey(currentToken)){
					count = tokens.get(currentToken);
					tokens.put(currentToken, count+1);
				}else{
					tokens.put(currentToken,1);
				}
			}
		}
	}
	
	/*
	 * Method for performing the cleaning 
	 */
	public static String cleanString(String str){
		String cleanedStr;
		//Convert to lowercase, 
		cleanedStr = str.toLowerCase();
		//Remove all html tags 
		cleanedStr = cleanedStr.replaceAll("\\<.*?>","");
		//Remove special characters from body
		cleanedStr = cleanedStr.replaceAll("[\\-\\\"\\+\\^:,()?*]"," ");
		cleanedStr = cleanedStr.replaceAll("[\\.']","");
		//Trim additional white spaces
		cleanedStr = cleanedStr.trim();
		return cleanedStr;
	}
	
	/*
	 * Method that extracts stop words from text files and adds them to the stop words set
	 
	public static Set<String> generateStopWords(){
		Set<String> stopWordsSet = new HashSet<>();
		//String fileNames[] = {"C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_6_en.txt","C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_5_en.txt","C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_4_google_en.txt","C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_3_en.txt","C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_2_en.txt","C:\\Users\\RaghuNandan\\Documents\\Workspace\\Hadoop\\Samples\\stop-words\\stop-words_english_1_en.txt"};
		try {
			for(String file:fileNames){
				BufferedReader br = new BufferedReader(new FileReader(file));
				String str;
				//Extract each line from the file
				while((str = br.readLine())!=null){
					stopWordsSet.add(str.toLowerCase());
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return stopWordsSet;
	}*/
	
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
