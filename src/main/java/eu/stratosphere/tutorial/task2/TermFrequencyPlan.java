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

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.tutorial.util.Util;

/**
 * Task 2: Plan for term frequency computation.
 */
public class TermFrequencyPlan implements PlanAssembler, PlanAssemblerDescription {

	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}

	@Override
	public Plan getPlan(String... args) {

		String inputPath = args.length >= 1 ? args[0] : "";
		String outputPath = args.length >= 2 ? args[1] : "";
		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");

		// - Task 2: Term Frequency -----------------------------------------------------------------------------------

		MapContract tfMapper = MapContract.builder(TermFrequencyMapper.class)
			.input(source)
			.name("Term Frequency Mapper")
			.build();

		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, tfMapper, "Term Frequencies");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0) // document ID
			.field(PactString.class, 1) // term d
			.field(PactInteger.class, 2); // term frequency

		Plan plan = new Plan(sink, "Term Frequency Computation");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}

	public static void main(String[] args) throws Exception {
		// Write test input to temporary directory
		String inputPath = Util.createTempDir("input");

		Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)");
		Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.");

		// Output
		// Replace this with your own path, e.g. "file:///path/to/results/"
//		String outputPath = Util.createTempDir("results");
		String outputPath = "file://"+System.getProperty("user.dir") +"/results_2";
		// Results should be:
		//
		// Document 1:
		// big 1
		// hello 1
		// stratosphere 1
		//
		// Document 2:
		// hello 1
		// big 2
		// data 1

		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new TermFrequencyPlan().getPlan(inputPath, outputPath);
		Util.executePlan(toExecute);

		Util.deleteAllTempFiles();
	}
}