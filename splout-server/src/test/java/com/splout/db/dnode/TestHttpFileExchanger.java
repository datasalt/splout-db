package com.splout.db.dnode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.HttpFileExchanger.ReceiveFileCallback;

public class TestHttpFileExchanger {

	public final static String TMP_FILE = "tmp-file-" + TestHttpFileExchanger.class.getName();
	public final static String TMP_DOWNLOAD_DIR = "tmp-download-" + TestHttpFileExchanger.class.getName();

	@Test
	public void test() throws IOException, InterruptedException {
		final File fileToSend = new File(TMP_FILE);

		String fileContents = "This is a text file. It is not very big, but it is ok for a test. "
		    + "Specially if we use a small buffer size to try to detect race conditions.";
		Files.write(fileContents.getBytes(), fileToSend);

		SploutConfiguration conf = SploutConfiguration.getTestConfig();
		conf.setProperty(FetcherProperties.DOWNLOAD_BUFFER, 16);
		conf.setProperty(FetcherProperties.TEMP_DIR, TMP_DOWNLOAD_DIR);

		final AtomicBoolean receivedOk = new AtomicBoolean(false);

		HttpFileExchanger exchanger = new HttpFileExchanger(conf, new ReceiveFileCallback() {
			@Override
			public void onFileReceived(File file) {
				receivedOk.set(true);
			}

			@Override
			public void onError(File file) {
			}

			@Override
			public void onBadCRC(File file) {
			}

			@Override
      public void onProgress(File file, long totalSize, long sizeDownloaded) {
	      
      }
		});
		Thread t = new Thread(exchanger);
		t.run();

		String dnodeHost = conf.getString(DNodeProperties.HOST);
		int httpPort = conf.getInt(HttpFileExchangerProperties.HTTP_PORT);

		exchanger.send(fileToSend, "http://" + dnodeHost + ":" + httpPort, true);

		final File downloadedFile = new File(TMP_DOWNLOAD_DIR, fileToSend.getName());

		new TestUtils.NotWaitingForeverCondition() {

			@Override
			public boolean endCondition() {
				return downloadedFile.exists() && downloadedFile.length() == fileToSend.length();
			}
		}.waitAtMost(5000);

		Assert.assertEquals(Files.toString(fileToSend, Charset.defaultCharset()),
		    Files.toString(downloadedFile, Charset.defaultCharset()));

		Assert.assertTrue(receivedOk.get());

		exchanger.close();
		t.join();

		fileToSend.delete();
		downloadedFile.delete();
		downloadedFile.getParentFile().delete();
	}
}
