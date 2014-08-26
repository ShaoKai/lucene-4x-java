package com.sky.lucene;

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sky.lucene.LuceneContextHolder.Domain;
import com.sky.lucene.LuceneContextHolder.IndexManager;

public class App {

	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException {
		LuceneContextHolder.getInstance();
		new Thread() {

			public void run() {
				while (true) {
					VoDocument voDocument = new VoDocument();
					voDocument.setDocumentId("A00001");
					voDocument.setCategory("SD");

					LuceneContextHolder.getInstance().getIndexManager(Domain.DOCUMENT).addVo(voDocument);

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
					VoDocument voDocument = new VoDocument();
					voDocument.setDocumentId("A00001");
					voDocument.setCategory("SD");

					LuceneContextHolder.getInstance().getIndexManager(Domain.DOCUMENT).addVo(voDocument);

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
					IndexManager docManager = LuceneContextHolder.getInstance().getIndexManager(Domain.DOCUMENT);
					Query query = docManager.createKeywordQuery("0");
					PageInfo<VoDocument> pageInfo = docManager.search(query, 1, 10);
					logger.info("Record Count : {}", pageInfo.getRecordCount());

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
