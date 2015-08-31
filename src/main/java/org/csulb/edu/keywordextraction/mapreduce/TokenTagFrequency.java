package org.csulb.edu.keywordextraction.mapreduce;

import java.util.Iterator;
import java.util.Map;

public class TokenTagFrequency {

	public static void recurseMap(Map tokenMap){
		
		    Iterator it = tokenMap.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        System.out.println(pair.getKey() + " = " + pair.getValue());
		        it.remove(); // avoids a ConcurrentModificationException
		    
		}
	}
}
