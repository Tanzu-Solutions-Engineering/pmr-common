package com.gopivotal.rcfile.validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RCFileValidator extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		RCFileInputFormat.setInputPaths(arg0, arg1);
		
	}

	public static void main(String[] args) {
		ToolRunner.run(new Configuration(), new RCFileValidator(), args);
	}
}