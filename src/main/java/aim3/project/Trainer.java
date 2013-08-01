package aim3.project;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.numericalmethod.suanshu.algebra.linear.matrix.doubles.Matrix;
import com.numericalmethod.suanshu.algebra.linear.matrix.doubles.matrixtype.dense.DenseMatrix;
import com.numericalmethod.suanshu.algebra.linear.matrix.doubles.matrixtype.sparse.SparseVector;
import com.numericalmethod.suanshu.algebra.linear.vector.doubles.Vector;
import com.numericalmethod.suanshu.algebra.linear.vector.doubles.dense.DenseVector;
import com.numericalmethod.suanshu.analysis.function.rn2r1.QuadraticFunction;
import com.numericalmethod.suanshu.graph.type.SparseDiGraph;
import com.numericalmethod.suanshu.misc.PrecisionUtils;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.constraint.linear.LinearEqualityConstraints;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.constraint.linear.LinearGreaterThanConstraints;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.convex.sdp.socp.qp.QPSolution;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.convex.sdp.socp.qp.activeset.QPPrimalActiveSetSolver;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.convex.sdp.socp.qp.activeset.QPPrimalActiveSetSolver.Solution;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.convex.sdp.socp.qp.problem.QPProblem;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecond;
//import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactMap;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;


public class Trainer implements PlanAssembler, PlanAssemblerDescription {

	public String getDescription() {
		return "Parameters: [noSubTasks] [input] [output]";
	}

	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String trainData = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		// Configure the training source:

		FileDataSource train = new FileDataSource(RecordInputFormat.class,
				trainData, "Training Data");
		train.setDegreeOfParallelism(noSubTasks);
//		train.getCompilerHints().setUniqueField(new FieldSet(0));
		RecordInputFormat.configureRecordFormat(train).recordDelimiter('\n')
				.fieldDelimiter('\t').field(DecimalTextIntParser.class, 0)
				.field(DecimalTextIntParser.class, 1)
				.field(VarLengthStringParser.class, 2)
				.field(DecimalTextIntParser.class, 3)
				.field(DecimalTextIntParser.class, 4);

		MapContract processTrain = MapContract.builder(ProcessInput.class)
				.input(train).name("Filter the training data").build();
		processTrain.setDegreeOfParallelism(noSubTasks);
		processTrain.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.1f);
		processTrain.getCompilerHints().setAvgBytesPerRecord(100);

		MatchContract joinTrain = MatchContract
				.builder(CreatePairs.class, PactInteger.class, 3, 3)
				.input1(processTrain).input2(processTrain)
				.name("Determine similarities for training").build();
		joinTrain.setDegreeOfParallelism(noSubTasks);

		ReduceContract trainSVMs = ReduceContract
				.builder(ReduceTrainer.class, PactInteger.class, 0)
				.input(joinTrain).name("Reduce and feed into the optimizer")
				.build();
		trainSVMs.setDegreeOfParallelism(noSubTasks);

		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				trainSVMs, "Training results");

		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter('\t').lenient(true).field(PactInteger.class, 0)
				.field(PactInteger.class, 1).field(PactDouble.class, 2);

		/*
		 * FileDataSink out = new FileDataSink(RecordOutputFormat.class,
		 * output,joinTrain, "The matrices");
		 * RecordOutputFormat.configureRecordFormat
		 * (out).recordDelimiter('\n').fieldDelimiter
		 * ('\t').lenient(true).field(PactInteger.class,
		 * 0).field(PactInteger.class, 1). field(PactInteger.class,
		 * 2).field(PactInteger.class, 3).field(PactInteger.class,
		 * 4).field(PactInteger.class, 5);
		 */

		Plan plan = new Plan(out, "SVM Trainer");
		plan.setDefaultParallelism(noSubTasks);

		return plan;
	}

	/**
	 * Processes the input PactRecord to only include the data we need. The
	 * output will be: ID, Sentiment, Message, TrainID
	 * 
	 * @author Igor Viskovic: igorv@mailbox.tu-berlin.de
	 * 
	 */
	@ConstantFields(value = { 0, 1, 2, 3 })
//	@OutCardBounds(lowerBound = 0, upperBound = 1)
	public static class ProcessInput extends MapStub {

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector)
				throws Exception {
			record.setNull(4);
			PactInteger p = record.getField(3, PactInteger.class);
			if (p.getValue() > 0)
				collector.collect(record);
			// Format: ID, Sent, Mes, TrainVal
		}

	}

	@ConstantFieldsFirst(value = { 0, 3 })
	@ConstantFieldsSecond(value = { 0, 1, 3 })
//	@OutCardBounds(lowerBound = 0, upperBound = 1)
	public static class CreatePairs extends MatchStub {
		@Override
		public void match(PactRecord first, PactRecord second,
				Collector<PactRecord> collector) throws Exception {
			// TODO Auto-generated method stub

			// First check if first.ID<=second.ID. We use the symmetry of the
			// matrix

		//	if (first.getField(0, PactInteger.class).getValue() <= second
		//			.getField(0, PactInteger.class).getValue()) {

				PactRecord out = new PactRecord(5);
				out.setField(0, first.getField(3, PactInteger.class));
				// The training value

				out.setField(2, first.getField(0, PactInteger.class));
				// The ID of the first element

				out.setField(3, second.getField(0, PactInteger.class));
				// The ID of the second element

				NGramKernel train = new NGramKernel(2, 6, 1);
				int v = train.calculate(first.getField(2, PactString.class)
						.toString(), second.getField(2, PactString.class)
						.toString());

				v = v * first.getField(1, PactInteger.class).getValue();

				v = v * second.getField(1, PactInteger.class).getValue();

				// We save the sentiments directly in v

				PactInteger i = new PactInteger(v);

				out.setField(1, i);

				out.setField(4, first.getField(1, PactInteger.class)); //
				// Sentiment of the first element
				out.setField(5, second.getField(1, PactInteger.class)); //
				// Sentiment of the second element
				collector.collect(out);
			}
		//}

	}

	public static class ReduceTrainer extends ReduceStub {

		@Override
		public void reduce(Iterator<PactRecord> values,
				Collector<PactRecord> collector) throws Exception {
			int size = 0;
			PactRecord rec = null;

			// We don't know how many training values we're gonna get, so we
			// store them in hash maps first

			HashMap<Integer, Integer> idToIndex = new HashMap<Integer, Integer>();
			// HashMap<Integer, Integer> indexToID=new HashMap<Integer,
			// Integer>();

			int trainval = 0;
			int firstID, secondID, val;

			HashMap<Integer, Integer> matrix = new HashMap<Integer, Integer>();
			HashMap<Integer, Integer> array = new HashMap<Integer, Integer>();

			int firstInd = 0, secondInd = 0;

			while (values.hasNext()) {
				rec = values.next();
				firstID = rec.getField(2, PactInteger.class).getValue();
				secondID = rec.getField(3, PactInteger.class).getValue();
				val = rec.getField(1, PactInteger.class).getValue();

				// Determine an index for the first ID if necessary
				if (!idToIndex.containsKey(firstID)) {
					idToIndex.put(firstID, size);
					firstInd= size++;
				} else {
					firstInd = idToIndex.get(firstID);
				}

				// Determine an index for the second ID if necessary
				if (!idToIndex.containsKey(secondID)) {
					idToIndex.put(secondID, size);
					secondInd=size++;
				} else {
					secondInd = idToIndex.get(secondID);
				}

				// Add the value to the matrix. We shouldn't encounter
				// duplicates, but we want to be sure
				if (!matrix.containsKey(firstInd * 10000 + secondInd)) {
					matrix.put(firstInd * 10000 + secondInd, val);
				}

				// Add the sentiment to the array, this is later used for the
				// bounds
				if (!array.containsKey(firstInd)) {
					array.put(firstInd, rec.getField(4, PactInteger.class)
							.getValue());
				}

				// Add the sentiment if necessary
				if (!array.containsKey(secondInd)) {
					array.put(secondInd, rec.getField(5, PactInteger.class)
							.getValue());
				}
			}

			// We now know the size of the matrix and can proceed to create it
			// and fill it

			double[][] mat = new double[size][size];

			Iterator it = matrix.entrySet().iterator();

			while (it.hasNext()) {
				Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>) it
						.next();
				firstInd = pairs.getKey() / 10000;
				secondInd = pairs.getKey() % 10000;
				mat[firstInd][secondInd] = (double) pairs.getValue();
				mat[secondInd][firstInd] = mat[firstInd][secondInd];
			/*	rec=new PactRecord(3);
				rec.setField(2, new PactDouble(mat[firstInd][secondInd]));
				rec.setField(0, new PactInteger(firstInd));
				rec.setField(1, new PactInteger(secondInd));
				collector.collect(rec); */
			}

			// Now mat contains all the values we'll need

			// This is the constraint that the sum of alpha_i * y_i is 0
			double[][] A = new double[1][size];

			// This is part of the goal function as SUM_{1,n} alpha_i . It
			// contains only -1, because the original problem is a maximization
			// problem and we have access to a minimizer
			double[] q = new double[size];
			for (int i = 0; i < size; i++) {
				q[i] = -1;
			}

			it = array.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>) it
						.next();
				A[0][pairs.getKey()] = (double)pairs.getValue();
			}
			
			Matrix H1 = new DenseMatrix(mat);
			Vector p = new DenseVector(q);
			
			QuadraticFunction F = new QuadraticFunction(H1, p);

			Matrix Adense = new DenseMatrix(A);
			Vector b = new DenseVector(new double[]{0});
			LinearEqualityConstraints EQUALITY = new LinearEqualityConstraints(Adense, b);
			
			double[][] ineq = new double[2 * size][size];
			double []bound = new double[2*size];
			for(int i=0;i<size;i++){
				//Takes care of the lower bound >=0
				ineq[i][i]=1;
				bound[i]=0;
				
				
				//Take care of the upper bound
				ineq[size+i][i]=-1;
				bound[i+size]=3000;
				
			}
			
			Matrix ineqDense = new DenseMatrix(ineq);
			Vector boundDense = new DenseVector(bound);
			LinearGreaterThanConstraints GREATER = new LinearGreaterThanConstraints(ineqDense, boundDense);
			
			QPProblem problem = new QPProblem(F, EQUALITY, GREATER, null);
			QPPrimalActiveSetSolver instance = new QPPrimalActiveSetSolver(PrecisionUtils.autoEpsilon(H1), 100000);
			Solution soln = instance.solve(problem);
			QPSolution qpSoln = soln.search();
			double[] result=qpSoln.minimizer().toArray();
			
		//	ConvexMultivariateRealFunction[] inequalities = new ConvexMultivariateRealFunction[2 * size];
			
			/*for (int i = 0; i < size; i++) {
				//Takes care of the lower bound >=0
				ine[i]=1.0;
				inequalities[i]=new LinearMultivariateRealFunction(ine, 0.0);
				
				// Takes care of the upper bound (<=C)
				ine[i]=-1.0;
				inequalities[i+size]=new LinearMultivariateRealFunction(ine, 10000.0);
				
				
				/*ineq[i][i] = -1;
				inequalities[i] = new LinearMultivariateRealFunction(ineq[i],
						1000);
				ineq[i + size][i] = 1;*/
				// Takes care of the lower bound
				/*inequalities[i + size] = new LinearMultivariateRealFunction(
						ineq[i + size], 0);*/
				
			//}

			//double[] result;
			// try {
			/*PDQuadraticMultivariateRealFunction objectiveFunction = new PDQuadraticMultivariateRealFunction(
					mat, q, 0);*/
			
			/*
			
			for(int i=0;i<size;i++){
				for(int j=i;j<size;j++){
					rec=new PactRecord(3);
					rec.setField(2, new PactDouble(mat[i][j]));
					rec.setField(0, new PactInteger(i));
					rec.setField(1, new PactInteger(j));
					collector.collect(rec);
				}
			}
			
			for(int i=0;i<size;i++){
				rec=new PactRecord(3);
				rec.setField(2, new PactDouble(A[0][i]));
				rec.setField(0, new PactInteger(i));
				rec.setField(1, new PactInteger(1000));
				collector.collect(rec);
				
				
			}*/
		/*	for(int i=0;i<2*size;i++){
				rec=new PactRecord(3);
				if(i<size)
					rec.setField(2, new PactDouble(10000));
				else rec.setField(2, new PactDouble(0));
				rec.setField(0, new PactInteger(i));
				rec.setField(1, new PactInteger((int)ineq[i][i%size]));
				collector.collect(rec);
			}
			*/
			
			
			/*
			//Set the objective function: minimize 0.5*xT*mat*x + qx + 0 <-> maximize -qx + 0.5*xT*mat*X +0
			OptimizationRequest or = new OptimizationRequest();
			or.setF0(objectiveFunction);
			// Set the inequalities
			//or.setFi(inequalities);
			//or.setInitialPoint(new double[size]);
			
			or.setNotFeasibleInitialPoint(new double[size]);
			
			//Array for SUM_{1,n} alpha_i * y_i
			or.setA(A);
			
			
			
			//B is 0 for the constraint SUM a_i*y_i = 0
			double[] b=new double[1];
			b[0]=0.0;
			or.setB(b);
			//or.setToleranceFeas(1.E-3);
			//or.setTolerance(1.E-3);

			JOptimizer opt = new JOptimizer();
			opt.setOptimizationRequest(or);
			int returnCode = opt.optimize();

			result = opt.getOptimizationResponse().getSolution();
			
			*/
			rec = new PactRecord(3);

			it = idToIndex.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>) it
						.next();
				firstID = pairs.getKey();
				if (result[pairs.getValue()] > 0)
					rec.setField(0, new PactInteger(trainval));
				rec.setField(1, new PactInteger(firstID));
				rec.setField(2, new PactDouble(result[pairs.getKey()]));
				collector.collect(rec);
			}
		
			/*
			 * } catch (Exception e) { for (int i = 0; i < size; i++) { if (i %
			 * 5 == 0) { // firstID = (trainval - 1) * size + i + 1; firstID = i
			 * * 80 + trainval; rec.setField(0, new PactInteger(trainval));
			 * rec.setField(1, new PactInteger(firstID)); rec.setField(2, new
			 * PactDouble(Math.random())); collector.collect(rec); } } }
			 */

		}
	}

}
