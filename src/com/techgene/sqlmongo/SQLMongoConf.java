package com.techgene.sqlmongo;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;


public class SQLMongoConf {
	   public static void main(String[] args) throws Exception
	   {
	     Configuration conf = new Configuration();
	     DBConfiguration.configureDB(conf,
	     "com.mysql.jdbc.Driver",   // driver class
	     "jdbc:mysql://localhost:3306/test", // db url
	     "root",    // user name
	     "root"); //password
         MongoConfigUtil.setOutputURI(conf, "mongodb://localhost:27017/test.dimstd");
         
	     Job job = new Job(conf);
	     job.setJarByClass(SQLMongoConf.class);
	     job.setMapperClass(SQLTableMapper.class);
	     
	     job.setMapOutputKeyClass(BSONWritable.class);
	     job.setMapOutputValueClass(BSONWritable.class);
	     
	     job.setInputFormatClass(DBInputFormat.class);
	     job.setOutputFormatClass(MongoOutputFormat.class);

	     DBInputFormat.setInput(
	     job,
	     SQLInputWritable.class,
	     "student",   //input table name
	     null,
	     null,
	     new String[] { "id", "name" }  // table columns
	     );

	     

	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
	}