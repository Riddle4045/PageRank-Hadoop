package PageRank;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PageRank {
	enum NCounterEnum {
		NVALUE;
	}

	/**
	 * This class will parse the wiki dump file
	 * 
	 *
	 */
	public static class OutLinkMap extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		Set<String> removeDuplicates;


		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
			String line = value.toString();
			removeDuplicates = new HashSet<String>();
			InputSource source = new InputSource(new StringReader(line));

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db;
			try {
				db = dbf.newDocumentBuilder();
				Document document = db.parse(source);
				XPathFactory xpathFactory = XPathFactory.newInstance();
				XPath xpath = xpathFactory.newXPath();

				String wikititle = xpath.evaluate("/page/title", document);
				wikititle = wikititle.replaceAll(" ", "_");
				String outlinks = xpath.evaluate("/page/revision/text", document);
				Pattern pattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
				Matcher matcher = pattern.matcher(outlinks);


				Text title = new Text(wikititle);

				output.collect(title, new Text("!"));
				while (matcher.find()) {

					String citation = matcher.group();
					citation = citation.replace("[[", "");
					citation = citation.replace("]]", "");
					try {
						citation = citation.split("\\|")[0].trim();

						word.set(citation);
						if(isValidWikiLink(citation)) {
							if(!removeDuplicates.contains(citation)) {
								citation = citation.replaceAll(" ", "_");
								removeDuplicates.add(citation);
								output.collect(new Text(citation), title);    
							}
						}
					}catch(ArrayIndexOutOfBoundsException e) {
						e.printStackTrace();
					}

				}    
				removeDuplicates.clear();

			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				e.printStackTrace();
			}
		}
	}    

	// This method is used to remove some deadlinks and some special wiki links like file: and image:
	private static boolean isValidWikiLink(String citation) {
		if ( citation.contains("{") || citation.contains("}") || 
				citation.contains("<") || citation.contains(">") || citation.contains("#")){
			return false;
		} else if ( citation.toLowerCase().contains("image:")){
			return false;
		} else if ( citation.toLowerCase().contains("file:")){
			return false;
		}else {
			return true;
		}
	}

	// This reducer is used to mark all the titles that have an account in wiki dump.
	// also collect all the inlinks for that title.
	public static class MarkWikiLinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {
			Set<String> outlinkSet = new HashSet<String>();
			boolean marker =  false;
			while ( value.hasNext()){
				String val = value.next().toString();
				if ( val.equals("!") ){
					marker  = true;        
				}else {
					outlinkSet.add(val);
				}
			}
			if ( marker ){
				output.collect(key, new Text("!"));
				for ( String index :  outlinkSet){
					output.collect(new Text(index), key);
				}

			}else {
				outlinkSet.clear();
			}
		}

	}
	
	// This mapper parses the output of the previous reducer. This does nothing.
	public static class RedlinksParser extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			int tabIndex = value.find("\t");
			String page = Text.decode(value.getBytes(), 0, tabIndex);
			//System.out.println(value.toString());
			String val = Text.decode(value.getBytes(), tabIndex+1,value.getLength()-(tabIndex+1));
			output.collect(new Text(page), new Text(val));
		}
	}


	// This reducer removes redlinks that do not have a page in the wiki dump.
	public static class RedLinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {
			String outlinks = "";
			int i  = 0 ;
			while ( value.hasNext()){
				String val = value.next().toString();
				if(!val.equals("!")) {
					if ( i == 0 ) {
						outlinks = val;
					}
					else{
						outlinks = outlinks + "\t" + val;
					}
					i++;
				}

			}
			//System.out.println(key.toString()+"\t"+"1.0\t"+outlinks);
			output.collect(key, new Text(outlinks));
		}

	}



	/** 
	 * Reads the output of the last reducer i.e job1 reducer from the disk thats all ...
	 * This has all the redlinks removed. This class is used to count the no of links 
	 * that are present in the wiki dump after removing deadlinks and no pages for some
	 * titles.
	 *
	 */
	public static class OutLinkParserMap extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			output.collect(new Text("N"),new LongWritable(1));
			//System.out.println("N "+1);
			reporter.incrCounter(NCounterEnum.NVALUE, 1);		
		}
	}

	// Counts the number of titles in the wiki dump and puts the final number in the file.
	public static class NReducer extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable>{
		long nValue =0;
		@Override
		public void reduce(Text key, Iterator<LongWritable> value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
						throws IOException {
			while(value.hasNext()){
				nValue = nValue + value.next().get() ;
				//	reporter.getCounter(NCounterEnum.NVALUE).increment(1);
			}
			output.collect(new Text("N ="), new LongWritable(nValue));
			//reporter.getCounter(NCounterEnum.NVALUE).increment(nValue);
		}
	}

	// This method is used to create an inlink map which will be later used to calculate 
	// page rank.
	public static class InLinksMap extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		long nValue = 0;
		int count= 0;
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			nValue = job.getLong("NVALUE", 0);
			count = job.getInt("COUNT", 0);
			//System.out.println("count = "+count);
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String[] value_string = value.toString().split("\t");
			String title = value_string[0];
			Double pageRank = 1.0 / nValue;
			int i = 1 ;
			if(count > 0) {
				pageRank = Double.parseDouble(value_string[1]);
				i++;
			}
			int num_outlinks = value_string.length-i;
			int init = i;
			String ooutputVal = "";
			while ( i < value_string.length) {
				//System.out.println(value_string[i]+"\t "+pageRank+"\t"+title+"\t"+num_outlinks);
				output.collect(new Text(value_string[i]), 
						new Text(pageRank+"\t"+title+"\t"+num_outlinks));
				if(i == init) {
					String val = value_string[i].replace("$%$", "");
					ooutputVal = ooutputVal+val;
				}else
					ooutputVal = ooutputVal+"\t"+value_string[i];

				i++;
			}   	
			//System.out.println(title+"\t"+"$%$"+ooutputVal);
			output.collect(new Text(title), new Text("$%$"+ooutputVal));		
		}
	}

	// Heres where we calculate the page rank.
	public static class RankReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text>{
		long nValue = 0;
		int count= 0;
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			nValue = job.getLong("NVALUE", 0);
			count = job.getInt("COUNT", 0);
			//System.out.println("count = "+count);
		}


		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			//nValue = reporter.getCounter(NCounterEnum.NVALUE).getValue();
			double newPageRank =  0.15/nValue;
			String outlinks = "";
			while ( value.hasNext()){
				Text val = value.next();
				if ( !val.toString().startsWith("$%$")){
					String[] rank_info = val.toString().split("\t");
					double pageRank = Double.parseDouble(rank_info[0]);
					//if(count == 0) {
					//	pageRank = pageRank/nValue;
					//}
					double num_links = Double.parseDouble(rank_info[2]);
					newPageRank = newPageRank + (pageRank/num_links)*0.85;
				} else {
					outlinks = val.toString().replace("$%$", "");			
				}
			}

			output.collect(key, new Text(Double .toString(newPageRank) +"\t"+outlinks));
		}
	}

	// This mapper and its corresponding reducer is used to put the pageranks in 
	// descending order.
	public static class RankingMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {

		double nValue = 0; 
		@Override
		public void configure(JobConf job) {

			super.configure(job);
			nValue = 5.0/(job.getLong("NVALUE", 0));
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, 
				Reporter arg3) throws IOException {

			String[] pageAndRank = getPageAndRank(value);
			Double parseDouble = Double.parseDouble(pageAndRank[1]);
			Text page = new Text(pageAndRank[0]);
			Text rank = new Text(""+parseDouble);
			if(parseDouble > nValue) {
				output.collect(rank, page);
			}

		}

		private String[] getPageAndRank( Text value) throws CharacterCodingException {

			String[] pageAndRank = new String[2];

			int tabPageIndex = value.find("\t");
			int tabRankIndex = value.find("\t", tabPageIndex + 1);

			// no tab after rank (when there are no links)
			int end;
			if (tabRankIndex == -1) {
				end = value.getLength() - (tabPageIndex + 1);
			} else {
				end = tabRankIndex - (tabPageIndex + 1);
			}
			pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
			pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

			return pageAndRank;
		}

	}

	public static class OrderReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {

			while(value.hasNext()){

				output.collect(value.next(), key);
			}
		}

	}

	public static class DoubleComparator extends DoubleWritable.Comparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static class FlipComparator extends WritableComparator {

		protected FlipComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			//consider only zone and day part of the key
			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			Double t1Item = Double.parseDouble(w1.toString());
			Double t2Item = Double.parseDouble(w2.toString());

			int comp = t1Item.compareTo(t2Item);

			return -comp;

		}
	}


	public static void main(String[] args) throws Exception {
		String ars1 = args[0];
		String ars2= args[1];
		
		//Start of Job1
		JobConf conf = new JobConf(PageRank.class);
		Path input = conf.getLocalPath("input");
		conf.setJobName("parse raw data");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(OutLinkMap.class);
		conf.setReducerClass(MarkWikiLinksReducer.class);
		conf.setNumReduceTasks(1);
		
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);


		FileInputFormat.setInputPaths(conf, new Path(ars1));
		FileOutputFormat.setOutputPath(conf, new Path(ars2+"/result/intermediateoutput"));

		Job job1 = new Job(conf); 
		job1.waitForCompletion(true);
		
		
		JobConf conf2 = removeRedLinks(ars2+"/result/intermediateoutput", ars2+"/result/PageRank.outlink.out");
		Job job2 = new Job(conf2);
		//FileSystem renameOutLinkGraphfs = FileSystem.get(URI.create(ars1+"/"), conf2); 
		job2.waitForCompletion(true);
		//renameOutLinkGraphfs.copyFromLocalFile(new Path(ars1+"/tmp/output1/part-00000"), new Path(ars1+"/results/PageRank.outlink.out"));
		//renameOutLinkGraphfs.close();

		 
		JobConf conf3 = addCountNJob(ars2+"/result/PageRank.outlink.out", ars2+"/result/PageRank.n.out");
		Job job3 = new Job(conf3);
		//FileSystem renameCountNfs = FileSystem.get(URI.create(ars1+"/"), conf3); 
		job3.waitForCompletion(true);
		//renameCountNfs.copyFromLocalFile(new Path(URI.create(ars1+"/tmp/output/part-00000")), new Path(URI.create(ars1+"/results/PageRank.n.out")));
		//renameCountNfs.close();
		
		long counter = job3.getCounters().findCounter(NCounterEnum.NVALUE)
				.getValue();
		String intermediateOutput = "";
		for(int i = 0; i < 8 ; i++) {
			
			JobConf confIter = computePageRank(ars2+"/result/PageRank.outlink.out"+(i== 0?"":i), ars2+"/result/PageRank.outlink.out"+(i+1), counter, i);
			Job jobIter = new Job(confIter);
			intermediateOutput = ars2+"/result/PageRank.outlink.out"+(i+1);
			jobIter.waitForCompletion(true);
			
			if(i == 0) {
				JobConf orderConf = orderRank(intermediateOutput, ars2+"/result/PageRank.iter1.out", counter);
				Job orderJob = new Job(orderConf);
				//FileSystem renameOrderFilefs = FileSystem.get(URI.create(ars1+"/"), orderConf);
				orderJob.waitForCompletion(true);
				//renameOrderFilefs.copyFromLocalFile(new Path(URI.create(ars1+"/tmp/order1/part-00000")),new Path(URI.create(ars1+"/results/PageRank.iter1.out")));
				//renameOrderFilefs.close();
			}
		}


		
		JobConf conf5 = orderRank(intermediateOutput, ars2+"/result/PageRank.iter8.out", counter);	
		Job job5 = new Job(conf5);
		//FileSystem renameOrderFilefs = FileSystem.get(URI.create(ars1+"/"), conf5);
		job5.waitForCompletion(true);
		//renameOrderFilefs.copyFromLocalFile(new Path(URI.create(ars1+"/tmp/order8/part-00000")),new Path(URI.create(ars1+"/results/PageRank.iter8.out")) );
		//renameOrderFilefs.close();
	}

	private static JobConf removeRedLinks(String inputFile, String outputFile) throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("count n");


		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(RedlinksParser.class);
		conf.setReducerClass(RedLinksReducer.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));
		
		return conf;
	}

	private static JobConf addCountNJob(String inputFile, String outputFile) throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("count n");


		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(OutLinkParserMap.class);
		//conf2.setCombinerClass(counter.class);
		conf.setReducerClass(NReducer.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));

		return conf;
	}

	private static JobConf computePageRank(String inputFile, String outputFile, long counter, int iterationCount) 
			throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("calculate pagrank");
		conf.setLong("NVALUE", counter);
		conf.setInt("COUNT", iterationCount);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(InLinksMap.class);
		conf.setReducerClass(RankReducer.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));

		return conf;
	}

	private static JobConf orderRank(String inputFile, String outFile, long counter) 
			throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("order values");
		conf.setLong("NVALUE", counter);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		

		conf.setMapperClass(RankingMapper.class);
		conf.setReducerClass(OrderReducer.class);
		conf.setOutputKeyComparatorClass(FlipComparator.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outFile));

		return conf;
	}

}
