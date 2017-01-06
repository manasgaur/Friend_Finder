package org.soundcloud;
import java.util.TreeSet;
import java.util.ArrayList;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import java.util.Arrays;
import java.util.Iterator;
public class SparkFriendCount {

	static class ProjectFn implements PairFunction<Tuple2<String, Tuple2<String, String>>, String, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		static final ProjectFn INSTANCE = new ProjectFn();

		@Override
		public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> triple) {
			return new Tuple2<>(triple._2()._2(), triple._2()._1());
		}
	}
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
			new FlatMapFunction<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<String> call(String s) throws Exception {
			return Arrays.asList(s.split("\n")).iterator();
		}
	};
	private static final PairFlatMapFunction<String, String, String> CREATE_FLATMAP =
			new PairFlatMapFunction<String, String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String, String>> call(String s) throws Exception {  
			String[] temp=s.split("\t");  
			ArrayList<Tuple2<String,String>> list=new ArrayList<Tuple2<String,String>>();  
			list.add(new Tuple2<String,String>(temp[0], temp[1]));
			list.add(new Tuple2<String,String>(temp[1], temp[0]));
			return list.iterator();  
		}
	};

	private static String getSortedVals(String key, Iterable<String> ans) {
		TreeSet<String> ts = new TreeSet<String>();
		Iterator<String> it= ans.iterator();
		while(it.hasNext())
		{
			String dummy = it.next();
			if (key.equals(dummy))
				continue;
			ts.add(dummy);
		}

		//  String []array = ts.stream().toArray(String[]::new);
		StringBuilder sb=new StringBuilder();
		sb.append(key);
		for(String n : ts) {
			sb.append("\t").append(n);
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Input file is expected as an input");
			System.exit(1);
		}
		if (args.length < 2 && args.length > 0) {
			System.err.println("Number of hops <i> is expected as an input");
			System.exit(1);
		}
		int iter = 0;
		try {
			iter = Integer.parseInt(args[1]);
		} catch (IllegalArgumentException iae) {
			System.err.println("<i> should be an integer");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(args[0]);
		JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR); // size of the file
		JavaPairRDD<String, String> tc = words.flatMapToPair(CREATE_FLATMAP);
		final JavaPairRDD<String, String> edges = tc.mapToPair(e -> new Tuple2<>(e._2(), e._1()));

		long nextCount = tc.count();
		long oldCount = 0l;
		// This constraint of nextCount != oldCount is used to prevent over computation
		// when the number of iteration given are extremely high
		while (nextCount != oldCount && iter > 1) {
			oldCount = nextCount;
			// Perform the join, obtaining an RDD of (y, (z, x)) pairs,
			// then project the result to obtain the new (x, z) paths.
			tc = tc.union(tc.join(edges).mapToPair(ProjectFn.INSTANCE)).distinct().cache();
			nextCount = tc.count();
			iter--;
		}
		JavaPairRDD<String,Iterable<String>> ans = tc.groupByKey();
		JavaRDD<String> ans1 = ans.map(e -> getSortedVals(e._1(), e._2()));
		ans1.saveAsTextFile("ans_output");
		sc.stop();
		sc.close();
	}