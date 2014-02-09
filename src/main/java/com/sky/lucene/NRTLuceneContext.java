package com.sky.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class NRTLuceneContext {
	public static final Version version = Version.LUCENE_40;
	private static final String indexFolder = "./index";
	private static NRTLuceneContext instance;
	private NRTManager nrtManager;
	private IndexWriter writer;
	private NRTManagerReopenThread reopenThread;

	private NRTLuceneContext() {
	}

	public void destory() {
		try {
			nrtManager.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		reopenThread.close();
	}

	private void init() {
		// Directory
		File folder = new File(indexFolder);
		SimpleFSDirectory directory = null;
		try {
			directory = new SimpleFSDirectory(folder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Writer
		try {
			IndexWriterConfig config = new IndexWriterConfig(version, new SimpleAnalyzer(version));
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			writer = new IndexWriter(directory, config);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			nrtManager = new NRTManager(new TrackingIndexWriter(writer), new SearcherFactory());
			reopenThread = new NRTManagerReopenThread(nrtManager, 1.0, 0.0); // maxStaleSec
																				// ,minStaleSec
			// reopenThread = new NRTManagerReopenThread(nrtManager, 5.0, 0.0);
			reopenThread.setDaemon(true);
			reopenThread.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static NRTLuceneContext getInstance() {
		if (instance == null) {
			instance = new NRTLuceneContext();
			instance.init();
		}
		return instance;
	}

	public IndexSearcher getSeacher() {
		return nrtManager.acquire();
	}

	public void releaseSearcher(IndexSearcher searcher) {
		try {
			nrtManager.release(searcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public NRTManager getNRTManager() {
		return nrtManager;
	}

	public void addDocument(Document doc) {
		try {
			writer.addDocument(doc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateDocument(Term term, Document doc) {
		try {
			writer.updateDocument(term, doc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void commit() {
		try {
			writer.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
