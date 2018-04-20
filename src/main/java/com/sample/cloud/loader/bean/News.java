package com.sample.cloud.loader.bean;

import java.util.Arrays;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

@DefaultCoder(AvroCoder.class)
public class News {
	private String title;
	private String description;
	private String date;
	private String id;
	private float score;
	private float magnitude;
	
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public float getMagnitude() {
		return magnitude;
	}
	public void setMagnitude(float magnitude) {
		this.magnitude = magnitude;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	   public String toString() {
		     return new ToStringBuilder(this).
		    		 append("id", id).
		       append("title", title).
		       append("description", description).
		       append("date", date).
		       append("score", score).
		       append("magnitude",magnitude).
		       toString();
		   }
	   
	   
     public static TableSchema getTableSchema() {
           return new TableSchema().setFields(Arrays.asList(
        		   				new TableFieldSchema().setName("id").setType("STRING"),
                           new TableFieldSchema().setName("title").setType("STRING"),
                           new TableFieldSchema().setName("description").setType("STRING"),
                           new TableFieldSchema().setName("date").setType("STRING"),
                           new TableFieldSchema().setName("score").setType("STRING"),
                           new TableFieldSchema().setName("magnitude").setType("FLOAT")));
   }

   public static TableRow toTableRow(News news) {
           return new TableRow()
                           .set("id", news.getId())
                           .set("title", news.getTitle())
                           .set("description", news.getDescription())
                           .set("date",news.getDate())
                           .set("score", news.getScore())
                           .set("magnitude",news.getMagnitude())
                           ;
   }
}
