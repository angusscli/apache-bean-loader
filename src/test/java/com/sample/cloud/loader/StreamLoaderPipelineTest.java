package com.sample.cloud.loader;


import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sample.cloud.loader.bean.News;

@RunWith(JUnit4.class)
public class StreamLoaderPipelineTest {
	private static final Logger log = LoggerFactory.getLogger(StreamLoaderPipelineTest.class);
	private Pipeline p;

	public static interface StreamOptions extends DataflowPipelineOptions {
		@Default.String("5")
		ValueProvider<String> getWindowsTime();
		void setWindowsTime(ValueProvider<String> value);
	}

	@Before
	public void init() throws Exception {
		StreamOptions options = TestPipeline.testingPipelineOptions().as(StreamOptions.class);
		options.setTempLocation("gs://va-test/staging");
		p = Pipeline.create(options);
	}
	
    
    public static class Print extends DoFn<Double, Double> {
    		private static final Logger log = LoggerFactory.getLogger(Print.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) {
	    		log.info("---->"+c.element());
	    		System.out.println(c.element());
	      c.output(c.element());
	    }
    } 

	//@Test
	public void test() {
		log.info("start");
		p.apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(StreamLoaderPipeline.SUBSCRIPTIONS))
		.apply(Window.into(SlidingWindows.of(Duration.standardMinutes(5)).every(Duration.standardSeconds(5))))
		.apply("ParseMsg", ParseJsons.of(News.class)).setCoder(AvroCoder.of(News.class))
		.apply(ParDo.of(new StreamLoaderPipeline.Convert()))
		.apply(Mean.<Double>globally().withoutDefaults())
		.apply(ParDo.of(new Print()))
		;

		p.run();
	}
	/*
	@Test
	public void test2() throws IOException, Exception {
		// Instantiate the Language client com.google.cloud.language.v1.LanguageServiceClient
		try (LanguageServiceClient language = LanguageServiceClient.create()) {
		  Document doc = Document.newBuilder()
		      .setContent("hello world")
		      .setType(Type.PLAIN_TEXT)
		      .build();
		  AnalyzeEntitiesRequest request = AnalyzeEntitiesRequest.newBuilder()
		      .setDocument(doc)
		      .setEncodingType(EncodingType.UTF16)
		      .build();

		  AnalyzeEntitiesResponse response = language.analyzeEntities(request);

		  // Print the response
		  for (Entity entity : response.getEntitiesList()) {
		    System.out.printf("Entity: %s", entity.getName());
		    System.out.printf("Salience: %.3f\n", entity.getSalience());
		    System.out.println("Metadata: ");
		    for (Map.Entry<String, String> entry : entity.getMetadataMap().entrySet()) {
		      System.out.printf("%s : %s", entry.getKey(), entry.getValue());
		    }
		    for (EntityMention mention : entity.getMentionsList()) {
		      System.out.printf("Begin offset: %d\n", mention.getText().getBeginOffset());
		      System.out.printf("Content: %s\n", mention.getText().getContent());
		      System.out.printf("Type: %s\n\n", mention.getType());
		    }
		  }
		}
	}*/

}
