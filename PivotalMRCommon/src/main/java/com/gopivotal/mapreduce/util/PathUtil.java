package com.gopivotal.mapreduce.util;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PathUtil {

	//private static final Logger LOG = Logger.getLogger(PathUtil.class);

	public static long getIdealSplitSize(FileSystem fs, Path[] input, int numMapTasks)
			throws IOException {

		long size = 0;

		for (Path status : input) {
			size += getIdealSplitSizeHelper(fs, fs.getFileStatus(status), 0);
		}

		return (long) ((float) size / (float) numMapTasks);
	}

	private static long getIdealSplitSizeHelper(FileSystem fs, FileStatus path, long accum)
			throws IOException {

		if (path.isFile()) {
			accum += path.getLen();
		} else {
			for (FileStatus status : fs.listStatus(path.getPath())) {
				accum += getIdealSplitSizeHelper(fs, status, accum);
			}
		}
		return accum;
	}
}
