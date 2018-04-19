package com.sample.cloud.loader;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sample.cloud.loader.bean.News;

public class StreamLoaderPipeline 
{

	private static final Logger log = LoggerFactory.getLogger(StreamLoaderPipeline.class);
	private static final String PROJECT_ID = "traded-risk-project-1";
	private static final String GS_TEMP = "gs://staging-testing-43541281/tmp/";
	private static final String JOB_NAME = "demo-stream-irc-pipeline";
	private static final SimpleDateFormat timestampdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String SUBSCRIPTIONS = "projects/traded-risk-project-1/subscriptions/news-subscription";
	

	private static final String BQ_TABLE_NAME = "traded-risk-project-1:poc.irc";

	/*
    private static class Convert extends DoFn<News, TableRow> {
	    @ProcessElement
	    public void processElement(ProcessContext c) throws UnsupportedEncodingException {
	      News e = c.element();
	      c.output(News.toTableRow(e));
	    }
    }*/
	
    public static class Convert extends DoFn<News, Double> {
	    @ProcessElement
	    public void processElement(ProcessContext c) throws UnsupportedEncodingException {
	      News e = c.element();
	      c.output(new Double(e.getScore()));
	    }
    }
    
    public static class Print extends DoFn<Double, Double> {
	    @ProcessElement
	    public void processElement(ProcessContext c) {
	    		log.info("---->"+c.element());
	      c.output(c.element());
	    }
    }
	
    public static void main( String[] args )
    {
        StreamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(StreamOptions.class);

		options.setProject(PROJECT_ID);
		options.setTempLocation(GS_TEMP);
		options.setGcpTempLocation(GS_TEMP);
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		options.setJobName(JOB_NAME + "-" + timestampdf.format(timestamp));

		p.apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(SUBSCRIPTIONS))
		.apply(Window.into(SlidingWindows.of(Duration.standardMinutes(5)).every(Duration.standardSeconds(5))))
		.apply("ParseMsg", ParseJsons.of(News.class)).setCoder(AvroCoder.of(News.class))
		.apply(ParDo.of(new Convert()))
		.apply(Mean.<Double>globally().withoutDefaults())
		.apply(ParDo.of(new Print()))
		
		/*
			.apply("MsgWindowSetup", Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply("ParseMsg", ParseJsons.of(News.class)).setCoder(AvroCoder.of(News.class))
            .apply(ParDo.of(new Convert()))
            */
		
            /*
            .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                  .to(BQ_TABLE_NAME)
                  .withSchema(News.getTableSchema())
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                  )
                  */
            ;

		
		p.run();
    }
    
    public static interface StreamOptions extends DataflowPipelineOptions {
        @Default.String("5")
        ValueProvider<String> getWindowsTime();
        void setWindowsTime(ValueProvider<String> value);
      }
}
