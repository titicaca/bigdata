/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.tutorial.task2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * This mapper computes the term frequency for each term in a document.
 * <p>
 * The term frequency of a term in a document is the number of times the term occurs in the respective document. If a
 * document contains a term three times, the term has a term frequency of 3 (for this document).
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The term frequency of "Big" in document 1 is 3 and 1 in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
public class TermFrequencyMapper extends MapStub {

	// ----------------------------------------------------------------------------------------------------------------
	// initialize reusable mutable objects

	/**
	 * Splits the document into terms and emits a PactRecord (docId, term, tf) for each term of the document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		// Implement your solution here
		Map<String,PactRecord> m = new HashMap<String,PactRecord>();

		String document = record.getField(0, PactString.class).toString();
		String lineCotent = document.substring(document.indexOf(",")+1);
		PactInteger docid = new PactInteger(Integer.parseInt(document.substring(0, document.indexOf(","))));
		
		lineCotent  = lineCotent.toLowerCase();
		// Implement your solution here
		
		PactString word = new PactString();
		for(int i = 0; i < lineCotent.length(); i ++ ){
			char c = lineCotent.charAt(i);
			if( c >= 'a' && c <= 'z'){
				word.append(c);
			}else{
				int length = word.toString().length();
				if(length>0){
					if(m.get(word.toString())==null){
						PactRecord p = new PactRecord();
						p.setField(0, docid);
						p.setField(1, word);
						p.setField(2, new PactInteger(1));
						m.put(word.toString(), p);
						//collector.collect(p);
					}else{
						PactRecord p = m.get(word.toString());
						p.setField(2, new PactInteger(p.getField(2, PactInteger.class).getValue()+1));
						m.put(word.toString(), p);
						//collector.collect(p);
					}	
					word = new PactString();
				}
			}
		}
		
		Iterator iter = m.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, PactRecord> entry = (Entry<String, PactRecord>) iter.next();
			PactRecord r = entry.getValue();
			collector.collect(r);
		}
		
	}
}