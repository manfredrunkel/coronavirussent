package com.manfred.corona;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;

public class CoronaDownloadNews implements Runnable {

	public void run() {
		loadFiles();
	}
	
	private static void loadFiles() {
		String csvFile = CoronaCounterMain.INPUT_MASTER_DIRECTORY_FILE;
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = " ";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			int i = 1;
			while ((line = br.readLine()) != null) {
				
				if(i % 96 == 0) {
					
					String[] values = line.split(cvsSplitBy);
					String url = values[2];
					String[] partsOfURL = url.split("/");
					String fileName = partsOfURL[partsOfURL.length - 1];

					File fileToDownload = new File(CoronaCounterMain.INPUT_DIRECTORY_DOWNLOAD + "/" + fileName);
					if (!fileToDownload.exists()) {
						FileUtils.copyURLToFile(new URL(url), fileToDownload);
						unzipFile(fileToDownload);
					}
				}
				
				
				i++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private static void unzipFile(File downloadedFile) throws IOException {
		byte[] buffer = new byte[1024];
		ZipInputStream zis = new ZipInputStream(new FileInputStream(downloadedFile));
		ZipEntry zipEntry = zis.getNextEntry();
		while (zipEntry != null) {
			File newFile = newFile(new File(CoronaCounterMain.INPUT_DIRECTORY), zipEntry);
			FileOutputStream fos = new FileOutputStream(newFile);
			int len;
			while ((len = zis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
			zipEntry = zis.getNextEntry();
		}
		zis.closeEntry();
		zis.close();
	}

	public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
		File destFile = new File(destinationDir, zipEntry.getName());

		String destDirPath = destinationDir.getCanonicalPath();
		String destFilePath = destFile.getCanonicalPath();

		if (!destFilePath.startsWith(destDirPath + File.separator)) {
			throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
		}

		return destFile;
	}

}
