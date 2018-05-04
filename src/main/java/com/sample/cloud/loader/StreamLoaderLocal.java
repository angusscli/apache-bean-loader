package com.sample.cloud.loader;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.language.v1.AnalyzeEntitiesRequest;
import com.google.cloud.language.v1.AnalyzeEntitiesResponse;
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;
import com.google.cloud.language.v1.Entity;
import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;
import com.sample.cloud.loader.bean.News;
import com.sample.cloud.loader.combine.NewsAggr;

public class StreamLoaderLocal 
{
	private static final Logger log = LoggerFactory.getLogger(StreamLoaderLocal.class);
	public static final String FROM_SUBSCRIPTIONS = "projects/traded-risk-project-1/subscriptions/news-subscription";
	public static final String TO_TOPIC = "projects/traded-risk-project-1/topics/db-topic";
	public static final String TO_TOPIC2 = "projects/traded-risk-project-1/topics/db2-topic";
	public static final String TO_TOPIC3 = "projects/traded-risk-project-1/topics/db3-topic";
	

	public static class ConvertEntities extends DoFn<News,String> {
		private static final Logger log = LoggerFactory.getLogger(ConvertEntities.class);
		
	    @ProcessElement
	    public void processElement(ProcessContext c) throws Exception {
	        News e = c.element();
	        String message;
			  if (e.getDescription()!=null && !"".equals(e.getDescription())) {
				  message = e.getTitle() + " " + e.getDescription();
			  } else {
				  message = e.getTitle();
			  }
			try (LanguageServiceClient language = LanguageServiceClient.create()) {
			  Document doc = Document.newBuilder()
			      .setContent(message.toLowerCase())
			      .setType(Type.PLAIN_TEXT)
			      .build();
			  AnalyzeEntitiesRequest request = AnalyzeEntitiesRequest.newBuilder()
			      .setDocument(doc)
			      .setEncodingType(EncodingType.UTF16)
			      .build();

			  AnalyzeEntitiesResponse response = language.analyzeEntities(request);

			  // Print the response

			  for (Entity entity : response.getEntitiesList()) {
				  if (entity.getSalience()>0.4) {
					  c.output(entity.getName());
				  }
			  }
	    }
	        
	}}
	
	public static class WordCloudTransform extends DoFn<KV<String, Long>, String> {
		private static final Logger log = LoggerFactory.getLogger(WordCloudTransform.class);

	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	    		KV<String, Long> kv = c.element();
	    		String output = "{\"key\":\""+kv.getKey()+"\",\"value\":\""+kv.getValue()+"\"}";
	    		log.info(output);
	    		c.output(output);
	    }
	}

	/*
    public static class Convert extends DoFn<News, Double> {
    		private static final Logger log = LoggerFactory.getLogger(Convert.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	      News e = c.element();

		  LanguageServiceClient language = LanguageServiceClient.create();

		  String message;
		  if (e.getDescription()!=null && !"".equals(e.getDescription())) {
			  message = e.getTitle() + " " + e.getDescription();
		  } else {
			  message = e.getTitle();
		  }
		  
		  Document lang = Document.newBuilder().setContent(message).setType(Type.PLAIN_TEXT).build();
		  
		  Sentiment sentiment = language.analyzeSentiment(lang).getDocumentSentiment();


	      c.output(new Double(sentiment.getScore()));
	    }
    }
    */
	
    public static class ParseStats extends DoFn<News, Double> {
		private static final Logger log = LoggerFactory.getLogger(ParseStats.class);
		    @ProcessElement
		    public void processElement(ProcessContext c) throws IOException {
		      News e = c.element();
		      c.output(new Double(e.getScore()));
		    }
	}

    public static class Convert extends DoFn<News, News> {
    		private static final Logger log = LoggerFactory.getLogger(Convert.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	      News e = c.element();

		  LanguageServiceClient language = LanguageServiceClient.create();

		  String message;
		  if (e.getDescription()!=null && !"".equals(e.getDescription())) {
			  message = e.getTitle() + " " + e.getDescription();
		  } else {
			  message = e.getTitle();
		  }
		  
		  Document lang = Document.newBuilder().setContent(message).setType(Type.PLAIN_TEXT).build();
		  
		  Sentiment sentiment = language.analyzeSentiment(lang).getDocumentSentiment();
		  
		  News news = new News();
		  news.setDate(e.getDate());
		  news.setDescription(e.getDescription());
		  news.setId(e.getId());
		  news.setMagnitude(sentiment.getMagnitude());
		  news.setScore(sentiment.getScore());
		  news.setTitle(e.getTitle());
		  news.setType(e.getType());

	      c.output(news);
	    }
    }

    public static class ConvertJson extends DoFn<News, String> {
    		private static final Logger log = LoggerFactory.getLogger(Convert.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	      News e = c.element();
	  		if (e.getScore()!=0) {
	  			String output = "{\"date\":\""+e.getDate()+"\",\"score\":\""+e.getScore()+"\",\"magnitude\":\""+e.getMagnitude()+"\"}";
		    		log.info(output);
		    		c.output(output);
	  		}
	    }
    }

    public static class Print extends DoFn<String, String> {
    		private static final Logger log = LoggerFactory.getLogger(Print.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) {
	    		log.info(c.element());
	    		c.output(c.element());
	    }
    }

	public static void main( String[] args )
    {
		StreamingNewsOptions options = PipelineOptionsFactory.fromArgs(args)
		        .withValidation()
		        .as(StreamingNewsOptions.class);
			options.setStreaming(true);
	    //options.setRunner(DirectRunner.class);

	    Pipeline p = Pipeline.create(options);
	    
	    PCollection<News> pNews = p.apply("ReadPubSub",PubsubIO.readStrings().fromSubscription(FROM_SUBSCRIPTIONS))
	    		.apply(Window.<String>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
	            .accumulatingFiredPanes())
	    		.apply("ParseMsg", ParseJsons.of(News.class)).setCoder(AvroCoder.of(News.class))
		;
	    
	    
	    PCollection<News> pConvertedNews = pNews.apply("NewsAggregate",ParDo.of(new Convert()))
	    		;
	    
	    pConvertedNews
	    		.apply("ConvertJson", ParDo.of(new ConvertJson()))
	    		.apply("WriteNewSubPub",PubsubIO.writeStrings().to(TO_TOPIC3))
	    ;

	    pConvertedNews.apply("ParseStats", ParDo.of(new ParseStats()))
	    		.apply("Mean",NewsAggr.<Double>globally().withoutDefaults())
	    		//.apply(ParDo.of(new Print()))
	    		.apply(PubsubIO.writeStrings().to(TO_TOPIC))
	    	;

	    pNews.apply("EntitiesAnalysis",ParDo.of(new ConvertEntities()))
	    		.apply(Count.perElement())
	    		.apply(ParDo.of(new WordCloudTransform()))
	    		.apply("WriteSubPubTopic",PubsubIO.writeStrings().to(TO_TOPIC2))
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
