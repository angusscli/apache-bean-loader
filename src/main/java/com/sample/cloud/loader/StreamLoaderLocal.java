package com.sample.cloud.loader;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sample.cloud.loader.bean.News;
import com.sample.cloud.loader.combine.NewsAggr;

public class StreamLoaderLocal 
{

	private static final Logger log = LoggerFactory.getLogger(StreamLoaderLocal.class);
	public static final String FROM_SUBSCRIPTIONS = "projects/traded-risk-project-1/subscriptions/news-subscription";
	public static final String TO_TOPIC = "projects/traded-risk-project-1/topics/db-topic";

/*
	private static final SimpleDateFormat timestampdf = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final String BQ_TABLE_NAME = "traded-risk-project-1:poc.irc";
*/
	
	/*
    private static class Convert extends DoFn<News, TableRow> {
	    @ProcessElement
	    public void processElement(ProcessContext c) throws UnsupportedEncodingException {
	      News e = c.element();
	      c.output(News.toTableRow(e));
	    }
    }*/
	
    public static class Convert extends DoFn<News, Double> {
    		private static final Logger log = LoggerFactory.getLogger(Convert.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) throws UnsupportedEncodingException {
	      News e = c.element();
	      c.output(new Double(e.getScore()));
	    }
    }
    
    public static class Print extends DoFn<String, String> {
    		private static final Logger log = LoggerFactory.getLogger(Print.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) {
	    		log.info("--->"+c.element());
	    		c.output(c.element());
	    }
    }

	public static void main( String[] args )
    {
		StreamingNewsOptions options = PipelineOptionsFactory.fromArgs(args)
		        .withValidation()
		        .as(StreamingNewsOptions.class);
			options.setStreaming(true);
	    options.setRunner(DirectRunner.class);

	    Pipeline p = Pipeline.create(options);
	    
	    PCollection<News> pNews = p.apply("ReadPubSub",PubsubIO.readStrings().fromSubscription(FROM_SUBSCRIPTIONS))
		.apply(Window.<String>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
	            //.withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
	            .accumulatingFiredPanes())
		.apply("ParseMsg", ParseJsons.of(News.class)).setCoder(AvroCoder.of(News.class))
		;
	    
	    pNews
		.apply("Convert",ParDo.of(new Convert()))
		.apply("Mean",NewsAggr.<Double>globally().withoutDefaults())
	    		.apply(ParDo.of(new Print()))
	    		.apply(PubsubIO.writeStrings().to(TO_TOPIC));
	    	;

	    p.run();
		
    }
        
    private interface StreamingNewsOptions extends StreamingOptions {
    		@Description("Path of the file to read from")
    		@Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    		String getInputFile();
    		void setInputFile(String value);
    	}
}
