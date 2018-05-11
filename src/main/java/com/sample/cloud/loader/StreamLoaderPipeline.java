package com.sample.cloud.loader;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

public class StreamLoaderPipeline 
{

	private static final Logger log = LoggerFactory.getLogger(StreamLoaderPipeline.class);
	private static final String PROJECT_ID = "techfest-hackathon-1";
	private static final String GS_TEMP = "gs://hackathon-staging-alphastock/tmp/";
	private static final String JOB_NAME = "alphastock-stream-loader-pipeline";
	private static final SimpleDateFormat timestampdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String FROM_SUBSCRIPTIONS = "projects/"+PROJECT_ID+"/subscriptions/news-subscription";
	public static final String TO_TOPIC = "projects/"+PROJECT_ID+"/topics/db-topic";
	public static final String TO_TOPIC2 = "projects/"+PROJECT_ID+"/topics/db2-topic";
	public static final String TO_TOPIC3 = "projects/"+PROJECT_ID+"/topics/db3-topic";

	public static class ConvertEntities extends DoFn<News,String> {
		private static final Logger log = LoggerFactory.getLogger(ConvertEntities.class);
		
	    @ProcessElement
	    public void processElement(ProcessContext c)  {
	    	try {
	        News e = c.element();
	        String message;
			  if (e.getDescription()!=null && !"".equals(e.getDescription())) {
				  message = e.getTitle() + " " + e.getDescription();
			  } else {
				  message = e.getTitle();
			  }
			  message = message.replace("\n","");
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
	    	} catch (Exception e) {
	    		
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
	    public void processElement(ProcessContext c) {
		    	
		    	try {
		      News e = c.element();
	
			  LanguageServiceClient language = LanguageServiceClient.create();
	
			  String message;
			  if (e.getDescription()!=null && !"".equals(e.getDescription())) {
				  message = e.getTitle() + " " + e.getDescription();
			  } else {
				  message = e.getTitle();
			  }
			  message = message.replace("\n","");
			  
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
		    	catch (Exception e) {
		    	}
	    }
    }

    public static class ConvertJson extends DoFn<News, String> {
    		private static final Logger log = LoggerFactory.getLogger(ConvertJson.class);
	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	      News e = c.element();
	  		if (e.getScore()!=0) {
	  			String title = e.getTitle().replace("\"", "").replace("'", "").replace("’","").replace('\r', ' ').replace('\n', ' '); //escapeHtml4(e.getTitle());
	  			//String title = URLEncoder.encode(e.getTitle());
	  			String output = "{\"type\":\""+e.getType()+"\",\"date\":\""+e.getDate()+"\",\"score\":\""+e.getScore()+"\",\"magnitude\":\""+e.getMagnitude()+"\",\"title\":\""+title+"\"}";
	  			//String output = "{\"type\":\""+e.getType()+"\",\"date\":\""+e.getDate()+"\",\"score\":\""+e.getScore()+"\",\"magnitude\":\""+e.getMagnitude()+"\"}";
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
		StreamingNewsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(StreamingNewsOptions.class);

                options.setProject(PROJECT_ID);
                options.setTempLocation(GS_TEMP);
                options.setGcpTempLocation(GS_TEMP);
                options.setRunner(DataflowRunner.class);
                options.setStreaming(true);
                options.setWorkerMachineType("n1-highmem-8");

                Pipeline p = Pipeline.create(options);

                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                options.setJobName(JOB_NAME + "-" + timestampdf.format(timestamp));

                
	    
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
        
    private interface StreamingNewsOptions extends DataflowPipelineOptions {
    		@Description("Path of the file to read from")
    		@Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    		String getInputFile();
    		void setInputFile(String value);
    	}
}
