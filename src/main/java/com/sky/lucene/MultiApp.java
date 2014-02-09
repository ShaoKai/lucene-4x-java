package com.sky.lucene;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.lucene.MultipleIndexLuceneContext.Domain;
import com.sky.lucene.MultipleIndexLuceneContext.IndexManager;

public class MultiApp {
	private static final Logger logger = LoggerFactory.getLogger(MultiApp.class);

	public static void main(String[] args) throws IOException {
		MultipleIndexLuceneContext.getInstance();
		new Thread() {
			public void run() {
				while (true) {
					VoDevice voDevice = new VoDevice();
					voDevice.setDeviceId("A00001");
					voDevice.setCategory("SD");

					MultipleIndexLuceneContext.getInstance().getIndexManager(Domain.DEVICE).addVo(voDevice);

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();
		new Thread() {
			public void run() {
				while (true) {
					VoDevice voDevice = new VoDevice();
					voDevice.setDeviceId("A00001");
					voDevice.setCategory("SD");

					MultipleIndexLuceneContext.getInstance().getIndexManager(Domain.DEVICE).addVo(voDevice);

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();

		new Thread() {
			public void run() {
				while (true) {
					IndexManager deviceManager = MultipleIndexLuceneContext.getInstance().getIndexManager(Domain.DEVICE);
					PageInfo<VoDevice> pageInfo = deviceManager.search("*:*", 1, 10);
					logger.info("Device Record Count : {}", pageInfo.getRecordCount());

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();

		new Thread() {
			public void run() {
				while (true) {
					VoDevice voDevice = new VoDevice();
					voDevice.setDeviceId("A00001");
					voDevice.setCategory("SD");

					MultipleIndexLuceneContext.getInstance().getIndexManager(Domain.DEVICE).addVo(voDevice);

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();

	}
	/**
	 * <pre>
	 * 	2014-02-09 20:23:09,163  INFO (MultiApp.java:63) - Device Record Count : 271
	 * 	2014-02-09 20:23:10,095  INFO (MultipleIndexLuceneContext.java:248) - Create Document ... start
	 * 	2014-02-09 20:23:10,103  INFO (MultipleIndexLuceneContext.java:251) - Create Document ... end<----
	 * 	2014-02-09 20:23:10,118  INFO (MultipleIndexLuceneContext.java:248) - Create Document ... start
	 * 	2014-02-09 20:23:10,128  INFO (MultipleIndexLuceneContext.java:251) - Create Document ... end<----
	 * 	2014-02-09 20:23:10,160  INFO (MultipleIndexLuceneContext.java:248) - Create Document ... start
	 * 	2014-02-09 20:23:10,164  INFO (MultipleIndexLuceneContext.java:140) - Search ... start
	 * 	2014-02-09 20:23:10,169  INFO (MultipleIndexLuceneContext.java:251) - Create Document ... end<----
	 * 	2014-02-09 20:23:10,179  INFO (MultipleIndexLuceneContext.java:172) - Search ... end<----
	 * 	2014-02-09 20:23:10,180  INFO (MultiApp.java:63) - Device Record Count : 274
	 * </pre>
	 */
}
