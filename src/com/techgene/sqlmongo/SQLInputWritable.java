package com.techgene.sqlmongo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;



public class SQLInputWritable  implements Writable, DBWritable
{
	   private int id;
	   private String name;

	   public void readFields(DataInput in) throws IOException {   }

	  

	   public void write(DataOutput out) throws IOException {  }

	  
	   public int getId()
	   {
	     return id;
	   }

	   public String getName()
	   {
	     return name;
	   }

	@Override
	public void readFields(java.sql.ResultSet rs) throws SQLException {
		// TODO Auto-generated method stub
		id = rs.getInt(1);
	     name = rs.getString(2);
	}

	@Override
	public void write(java.sql.PreparedStatement ps) throws SQLException {
		// TODO Auto-generated method stub
		  ps.setInt(1, id);
		     ps.setString(2, name);
	}
	}