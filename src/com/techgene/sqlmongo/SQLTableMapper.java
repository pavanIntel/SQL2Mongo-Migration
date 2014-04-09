package com.techgene.sqlmongo;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;

public class SQLTableMapper extends Mapper<LongWritable, SQLInputWritable, BSONWritable, BSONWritable>
{
	   

	   protected void map(LongWritable id, SQLInputWritable value, Context ctx)
	   {
	     try
	     {
	        String name = value.getName();
	        int ids=value.getId();
            BSONObject valueobj=BasicDBObjectBuilder.start().add("id", ids).add("name", name).get();
	        BSONWritable mongo=new BSONWritable(valueobj);
	        BSONObject keyobj=BasicDBObjectBuilder.start().add("_id", ids).get();
	        BSONWritable key=new BSONWritable(keyobj);
	        ctx.write(key,mongo);
	        
	     } 
	     catch(IOException e)
	     {
	        e.printStackTrace();
	     } catch(InterruptedException e)
	     {
	        e.printStackTrace();
	     }
	   }
	}
