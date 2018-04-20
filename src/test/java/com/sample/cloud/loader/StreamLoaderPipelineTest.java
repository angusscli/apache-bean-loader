package com.sample.cloud.loader;


import java.io.IOException;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.language.v1.AnalyzeEntitiesRequest;
import com.google.cloud.language.v1.AnalyzeEntitiesResponse;
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;
import com.google.cloud.language.v1.Entity;
import com.google.cloud.language.v1.EntityMention;
import com.google.cloud.language.v1.LanguageServiceClient;

@RunWith(JUnit4.class)
public class StreamLoaderPipelineTest {
	private static final Logger log = LoggerFactory.getLogger(StreamLoaderPipelineTest.class);
	private Pipeline p;

	/*
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
	*/
    
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
    /*
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
	}*/

	
	@Test
	public void test2() throws IOException, Exception {
		// Instantiate the Language client com.google.cloud.language.v1.LanguageServiceClient
		try (LanguageServiceClient language = LanguageServiceClient.create()) {
		  Document doc = Document.newBuilder()
		      .setContent("Investors are ignoring one of the biggest risks to the market, strategist says")
		      .setType(Type.PLAIN_TEXT)
		      .build();
		  AnalyzeEntitiesRequest request = AnalyzeEntitiesRequest.newBuilder()
		      .setDocument(doc)
		      .setEncodingType(EncodingType.UTF16)
		      .build();

		  AnalyzeEntitiesResponse response = language.analyzeEntities(request);

		  // Print the response
		  for (Entity entity : response.getEntitiesList()) {
		    System.out.printf("Entity: %s\n", entity.getName());
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
	}
}
