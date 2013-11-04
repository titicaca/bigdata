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
package eu.stratosphere.tutorial.task1;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * This reducer is part of the document frequency computation. See {@link DocumentFrequencyMapper} for an explanation
 * of what the document frequency is.
 * <p>
 * The reduce method will be called grouped by each term.
 */
@Combinable
@ConstantFields(0)
public class DocumentFrequencyReducer extends ReduceStub {

	// ----------------------------------------------------------------------------------------------------------------
	//private final PactInteger cnt = new PactInteger();
	
	/**
	 * Adds up all (term, 1) records emitted by {@link DocumentFrequencyMapper} grouped for each term.
	 * <p>
	 * If the inputs are records (big, 1) and (big, 1), the reduce method will be called with an iterator over both
	 * records.
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {
		// Implement your solution here
//		java.util.Map<String, Integer> map = new java.util.HashMap<String, Integer>();
//		boolean isExist = true;
//		while(records.hasNext()){
//			PactRecord record = records.next();
//			String word = record.getField(0, PactString.class).toString();
//			if(!map.containsKey(word)){
//				isExist = false;
//				map.put(word, 1);
//			}else{
//				isExist = true;
//				map.put(word, (map.get(word)+1));
//			}		
//		}
//		Iterator iter = map.entrySet().iterator();
//		while(iter.hasNext()){
//			Map.Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
//			PactRecord record = new PactRecord();
//			record.setField(0, new PactString(entry.getKey()));
//			record.setField(1, new PactInteger(entry.getValue()));
//			collector.collect(record);
//		}
//		while(records.hasNext()){
//			PactRecord record = records.next();
//			collector.collect(record);
//		}
		
		 PactRecord element = null;
         int sum = 0;
         while (records.hasNext()) {
                 element = records.next();
                 PactInteger i = element.getField(1, PactInteger.class);
                 sum += i.getValue();
                 //sum++;
         }

         //this.cnt.setValue(sum);
         //element.setField(1, this.cnt);
         element.setField(1, new PactInteger(sum));

         collector.collect(element);

	}
}
