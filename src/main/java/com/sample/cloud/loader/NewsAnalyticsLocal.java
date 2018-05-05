package com.sample.cloud.loader;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;
import com.sample.cloud.loader.bean.News;

public class NewsAnalyticsLocal {
	private static final Logger log = LoggerFactory.getLogger(NewsAnalyticsLocal.class);

	public static class Print extends DoFn<String, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			log.info("" + c.element());
			c.output(c.element());
		}
	}

	public static class Analysis extends DoFn<News, String> {
		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			News e = c.element();

			LanguageServiceClient language = LanguageServiceClient.create();

			String message;
			if (e.getDescription() != null && !"".equals(e.getDescription())) {
				message = e.getTitle() + " " + e.getDescription();
			} else {
				message = e.getTitle();
			}

			Document lang = Document.newBuilder().setContent(message).setType(Type.PLAIN_TEXT).build();

			Sentiment sentiment = language.analyzeSentiment(lang).getDocumentSentiment();
			StringBuffer sb = new StringBuffer();
			sb.append(e.getDate()).append("|")
			.append(e.getId()).append("|")
			.append(e.getTitle()).append("|")
			.append(sentiment.getScore()).append("|")
			.append(sentiment.getMagnitude());

			c.output(sb.toString());
		}
	}

	public static void main(String[] args) {
		BatchNewsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchNewsOptions.class);
		options.setStreaming(true);

		Pipeline p = Pipeline.create(options);
		String date = "20180504";

		p.apply(TextIO.read().from(options.getInputFile()+date+"/" + date + "*news*"))
				.apply("ParseMsg", ParseJsons.of(News.class))
				.setCoder(AvroCoder.of(News.class))
				.apply(ParDo.of(new Analysis()))
				.apply(ParDo.of(new Print()))
				.apply(TextIO.write().to(options.getOutputFile()+date+".txt").withoutSharding());
				;

		p.run();

	}

	private interface BatchNewsOptions extends StreamingOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://staging-testing-43541281/data/")
		String getInputFile();

		void setInputFile(String value);

		@Default.String("gs://staging-testing-43541281/output/")
		String getOutputFile();

		void setOutputFile(String value);
	}
}
