package com.sky.lucene;

import java.io.IOException;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException {
		LuceneContext.getInstance();
		new Thread() {
			public void run() {
				while (true) {
					try {
						IndexSearcher searcher = LuceneContext.getInstance().getSeacher();
						TermQuery query = new TermQuery(new Term("id", "A00001"));
						TopDocs tds = searcher.search(query, 30);
						logger.info("1. Docs Size : {}", tds.totalHits);
						LuceneContext.getInstance().releaseSearcher(searcher);
					} catch (IOException e) {
						e.printStackTrace();
					}
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
					try {
						TermQuery query = new TermQuery(new Term("id", "A00001"));
						IndexSearcher searcher = LuceneContext.getInstance().getSeacher();
						TopDocs tds = searcher.search(query, 30);
						LuceneContext.getInstance().releaseSearcher(searcher);
						logger.info("2. Docs Size : {}", tds.totalHits);
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}.start();

		int ms = (RandomUtils.nextInt(10) + 1) * 1000;
		logger.info("3. ms : {}", ms);
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		new Thread() {
			public void run() {
				Document doc = new Document();
				doc.add(new StringField("id", "A00001", Store.YES));
				doc.add(new StringField("status", "1", Store.YES));
				doc.add(new StringField("statusName", "WaitForAudit", Store.YES));
				LuceneContext.getInstance().addDocument(doc);
				LuceneContext.getInstance().commit();
				logger.info("3. Added Document : {}", "A00001");
			}
		}.start();
	}
}
