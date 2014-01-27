package com.gopivotal.mapreduce.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Contains a number of helpful Path-based utilities.
 */
public class PathUtil {

	/**
	 * Gets the ideal split size for the array of paths. Recursively goes
	 * through the paths and counts all the bytes. Then uses this information
	 * and the given number of map tasks to return the ideal size.
	 * 
	 * @param fs
	 *            The FileSystem to use
	 * @param input
	 *            An array of file paths (or directories) to collect file size.
	 * @param numMapTasks
	 *            The number of map tasks you want to use for this job.
	 * @return The ideal split size to create the given <code>numMapTasks</code>
	 *         .
	 * @throws IOException
	 */
	public static long getIdealSplitSize(FileSystem fs, Path[] input,
			int numMapTasks) throws IOException {

		long size = 0;

		for (Path status : input) {
			size += getIdealSplitSizeHelper(fs, fs.getFileStatus(status), 0);
		}

		return (long) ((float) size / (float) numMapTasks);
	}

	private static long getIdealSplitSizeHelper(FileSystem fs, FileStatus path,
			long accum) throws IOException {

		if (path.isFile()) {
			accum += path.getLen();
		} else {
			for (FileStatus status : fs.listStatus(path.getPath())) {
				accum += getIdealSplitSizeHelper(fs, status, accum);
			}
		}
		return accum;
	}

	/**
	 * Recursively goes through the given array of paths, populating
	 * <code>destList</code> with all paths underneath.
	 * 
	 * @param fs
	 *            The {@link FileSystem} to use
	 * @param destList
	 *            The destination list to hold the paths
	 * @param paths
	 *            One or more {@link Path}s to search.
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void getAllPaths(FileSystem fs, List<Path> destList,
			String... paths) throws FileNotFoundException, IOException {

		for (String path : paths) {
			getAllPathsHelper(fs, destList, fs.getFileStatus(new Path(path)));
		}
	}

	private static void getAllPathsHelper(FileSystem fs, List<Path> destList,
			FileStatus status) throws FileNotFoundException, IOException {
		if (status.isDirectory()) {
			for (FileStatus s : fs.listStatus(status.getPath())) {
				getAllPathsHelper(fs, destList, s);
			}
		} else {
			destList.add(status.getPath());
		}
	}
}
