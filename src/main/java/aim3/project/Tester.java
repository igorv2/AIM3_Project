package aim3.project;

import java.util.Iterator;

import aim3.project.utils.kernels.stringKernels.NGramKernel;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
//import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

public class Tester implements PlanAssemblerDescription, PlanAssembler {

	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String trainInput = (args.length > 1 ? args[1] : "");
		String testInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// Configure the training source:

		FileDataSource train = new FileDataSource(RecordInputFormat.class,
				trainInput, "Training results");
		train.setDegreeOfParallelism(noSubTasks);
//		train.getCompilerHints().setUniqueField(new FieldSet(1));
		RecordInputFormat.configureRecordFormat(train).recordDelimiter('\n')
				.fieldDelimiter('\t').field(DecimalTextIntParser.class, 0)
				.field(DecimalTextIntParser.class, 1)
				.field(DecimalTextDoubleParser.class, 2);

		// Configure the test source:
		FileDataSource test = new FileDataSource(RecordInputFormat.class,
				testInput, "Test Data");
		test.setDegreeOfParallelism(noSubTasks);
		test.getCompilerHints().addUniqueField(new FieldSet(0));
		RecordInputFormat.configureRecordFormat(test).recordDelimiter('\n')
				.fieldDelimiter('\t').field(DecimalTextIntParser.class, 0)
				.field(DecimalTextIntParser.class, 1)
				.field(VarLengthStringParser.class, 2)
				.field(DecimalTextIntParser.class, 3)
				.field(DecimalTextIntParser.class, 4);

		MapContract processTrain = MapContract.builder(ProcessTrainData.class)
				.input(train).name("Filter the training data").build();
		processTrain.setDegreeOfParallelism(noSubTasks);
		processTrain.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		processTrain.getCompilerHints().setAvgBytesPerRecord(16);

		MapContract processTest = MapContract.builder(ProcessTestData.class)
				.input(test).name("Filter the test data").build();
		processTest.setDegreeOfParallelism(noSubTasks);
		processTrain.getCompilerHints().setAvgBytesPerRecord(120);
		processTrain.getCompilerHints().addUniqueField(
				new FieldSet(new int[] { 0 }));

		MatchContract joinTrain = MatchContract
				.builder(JoinTrain.class, PactInteger.class, 1, 0)
				.input1(processTrain).input2(processTest)
				.name("Get the messages for the training data").build();
		joinTrain.setDegreeOfParallelism(noSubTasks);

		/*
		
		MatchContract joinTrainAndTest = MatchContract
				.builder(JoinTrainAndTest.class, PactInteger.class, 0, 3)
				.input1(joinTrain).input2(processTest)
				.name("Determine K(train_i, test_j)").build();
		joinTrainAndTest.setDegreeOfParallelism(noSubTasks);

		*/
		
		CrossContract joinTrainAndTest = CrossContract
				.builder(CrossTrainAndTrest.class)
				.input1(joinTrain).input2(processTest)
				.name("Determine K(train_i, test_j)").build();
		joinTrainAndTest.setDegreeOfParallelism(noSubTasks);

		ReduceContract reduceByID = ReduceContract
				.builder(ReduceForID.class, PactInteger.class, 1)
				.input(joinTrainAndTest).name("Aggregate sum for every test")
				.build();
		reduceByID.setDegreeOfParallelism(noSubTasks);

		ReduceContract reduceByTrain = ReduceContract
				.builder(ReduceForTrain.class, PactInteger.class, 0)
				.input(reduceByID).name("Determine accuracy for every testval")
				.build();

		reduceByTrain.setDegreeOfParallelism(noSubTasks);

		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				reduceByTrain, "Group accuracy");

		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter('\t').lenient(true).field(PactInteger.class, 0)
				.field(PactInteger.class, 1).field(PactInteger.class, 2);

		Plan plan = new Plan(out, "SVM Tester");
		plan.setDefaultParallelism(noSubTasks);

		return plan;
	}

	public String getDescription() {
		return "Parameters: [noSubTasks] [input:TrainData] [input:TestData] [output]";
	}

	@ConstantFields(value = { 0, 1, 2, 3, 4 })
//	@OutCardBounds(lowerBound = 0, upperBound = 1)
	public static class ProcessTestData extends MapStub {

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector)
				throws Exception {
			PactInteger p = record.getField(4, PactInteger.class);
			if (p.getValue() > 0)
				collector.collect(record);

		}

	}

	@ConstantFields(value = { 0, 1, 2 })
	public static class ProcessTrainData extends MapStub {

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector)
				throws Exception {
			collector.collect(record);

		}

	}

	@ConstantFieldsSecondExcept(value = { 4 })
//	@OutCardBounds(lowerBound = 1, upperBound = 1)
	public static class JoinTrain extends MatchStub {
		@Override
		public void match(PactRecord first, PactRecord second,
				Collector<PactRecord> collector) throws Exception {
			second.setField(4, first.getField(2, PactDouble.class));
			collector.collect(second);

		}
	}

	public static class JoinTrainAndTest extends MatchStub {
		PactRecord rec = new PactRecord(4);

		@Override
		public void match(PactRecord first, PactRecord second,
				Collector<PactRecord> collector) throws Exception {
			// TODO Auto-generated method stub
			NGramKernel train = new NGramKernel(2, 6, 3);
			double v = train.calculate(first.getField(2, PactString.class)
					.toString(), second.getField(2, PactString.class)
					.toString());
			v = v * first.getField(4, PactDouble.class).getValue()*first.getField(1, PactInteger.class).getValue();
			// TrainVal, ID, sent, val

			rec.setField(0, first.getField(3, PactInteger.class));
			rec.setField(1, new PactInteger(second.getField(0, PactInteger.class).getValue()+first.getField(3, PactInteger.class).getValue()*8000));
			rec.setField(2, second.getField(1, PactInteger.class));
			rec.setField(3, new PactDouble(v));
			collector.collect(rec);
		}
	}

	public static class CrossTrainAndTrest extends CrossStub {
		PactRecord rec = new PactRecord(4);

		@Override
		public void cross(PactRecord first, PactRecord second,
				Collector<PactRecord> collector) {
			// TODO Auto-generated method stub
			if (first.getField(3, PactInteger.class).getValue() != second
					.getField(3, PactInteger.class).getValue()) {
				NGramKernel train = new NGramKernel(2, 6, 3);
				double v = train.calculate(first.getField(2, PactString.class)
						.toString(), second.getField(2, PactString.class)
						.toString());
				v = v * first.getField(4, PactDouble.class).getValue()*first.getField(1, PactInteger.class).getValue();
				// TrainVal, ID, sent, val

				rec.setField(0, first.getField(3, PactInteger.class));
				rec.setField(1, new PactInteger(second.getField(0, PactInteger.class).getValue()+first.getField(3, PactInteger.class).getValue()*8000));
				rec.setField(2, second.getField(1, PactInteger.class));
				rec.setField(3, new PactDouble(v));
				collector.collect(rec);
			}
		}

	}

	@ConstantFields(value = { 0 })
//	@OutCardBounds(lowerBound = 1, upperBound = 1)
	public static class ReduceForID extends ReduceStub {
		PactRecord rec;
		PactInteger one = new PactInteger(1);
		PactInteger zero = new PactInteger(0);
		PactRecord out = new PactRecord(3);

		@Override
		public void reduce(Iterator<PactRecord> iterator,
				Collector<PactRecord> collector) throws Exception {
			double sum = 0;
			int sent = 0;
			PactInteger train = null;
			while (iterator.hasNext()) {
				rec = iterator.next();
				if (train == null)
					train = rec.getField(0, PactInteger.class);
				if (sent == 0)
					sent = rec.getField(2, PactInteger.class).getValue();
				sum = sum + rec.getField(3, PactDouble.class).getValue();
			}
			out.setField(0, train);
			if (sum * sent > 0) {
				out.setField(1, this.one);
				out.setField(2, this.zero);
			} else {
				out.setField(1, this.zero);
				out.setField(2, this.one);
			}
			collector.collect(out);
		}

	}

	@ConstantFields(value = { 0 })
//	@OutCardBounds(lowerBound = 1, upperBound = 1)
	@Combinable
	public static class ReduceForTrain extends ReduceStub {
		PactRecord rec;
		PactRecord out = new PactRecord(3);

		@Override
		public void reduce(Iterator<PactRecord> iterator,
				Collector<PactRecord> collector) throws Exception {
			int sumGood = 0;
			int sumBad = 0;
			PactInteger train = null;

			while (iterator.hasNext()) {
				rec = iterator.next();
				if (train == null)
					train = rec.getField(0, PactInteger.class);
				sumGood += rec.getField(1, PactInteger.class).getValue();
				sumBad += rec.getField(2, PactInteger.class).getValue();
			}
			out.setField(0, train);
			out.setField(1, new PactInteger(sumGood));
			out.setField(2, new PactInteger(sumBad));
			collector.collect(out);
		}

		@Override
		public void combine(Iterator<PactRecord> iterator,
				Collector<PactRecord> collector) throws Exception {
			this.reduce(iterator, collector);
		}
	}

}
