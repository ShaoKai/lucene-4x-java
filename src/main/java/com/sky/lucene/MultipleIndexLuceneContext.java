package com.sky.lucene;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class MultipleIndexLuceneContext {
	private static final Logger logger = LoggerFactory.getLogger(MultipleIndexLuceneContext.class);
	public static final Version LUCENE_VERSION = Version.LUCENE_40;
	public static final String ROOT_PATH = "./index/";
	private static MultipleIndexLuceneContext instance;
	private Map<Domain, IndexManager> indexMap;

	private MultipleIndexLuceneContext() {
	}

	private void init() {
		indexMap = new HashMap<Domain, IndexManager>();

		for (Domain domain : Domain.values()) {
			indexMap.put(domain, new IndexManager(domain));
		}
	}

	public void destoryAll() {
		for (Domain domain : Domain.values()) {
			indexMap.get(domain).destory();
		}
	}

	public IndexManager getIndexManager(Domain domain) {
		return indexMap.get(domain);
	}

	public static MultipleIndexLuceneContext getInstance() {
		if (instance == null) {
			instance = new MultipleIndexLuceneContext();
			instance.init();
		}
		return instance;
	}

	enum Domain {
		DEVICE(VoDevice.class), MATERIAL(VoMaterial.class); // , TASK

		private Class voClass;

		Domain(Class voClass) {
			this.voClass = voClass;
		}

		protected Class getVoClass() {
			return voClass;
		}
	}

	class IndexManager {

		private Domain domin;
		private NRTManager nrtManager;
		private IndexWriter writer;
		private NRTManagerReopenThread reopenThread;

		public IndexManager(Domain domain) {
			this.domin = domain;
			File folder = new File(ROOT_PATH + domain.name());
			SimpleFSDirectory directory = null;
			try {
				directory = new SimpleFSDirectory(folder);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Writer
			try {
				IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, new SimpleAnalyzer(LUCENE_VERSION));
				config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
				writer = new IndexWriter(directory, config);
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				nrtManager = new NRTManager(new TrackingIndexWriter(writer), new SearcherFactory());
				// maxStaleSec ,minStaleSec
				reopenThread = new NRTManagerReopenThread(nrtManager, 1.0, 0.0);
				reopenThread.setDaemon(true);
				reopenThread.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// private ScoreDoc getLastDoc(int pageOffset, IndexSearcher searcher,
		// Query query) {
		// if (pageOffset <= 0) {
		// return null;
		// }
		// TopDocs docs = null;
		// try {
		// docs = searcher.search(query, pageOffset - 1);
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// return docs.scoreDocs[pageOffset - 1];
		// }

		public PageInfo search(String queryString, int page, int pageSize) {
			logger.info("Search ... start");
			IndexSearcher searcher = null;
			PageInfo pageInfo = null;

			QueryParser parser = new QueryParser(LUCENE_VERSION, "", new SimpleAnalyzer(LUCENE_VERSION));
			parser.setAllowLeadingWildcard(true);
			parser.setLowercaseExpandedTerms(false);
			try {
				Query query = parser.parse(queryString);
				searcher = nrtManager.acquire();
				TopDocs docs = searcher.search(query, 100000);
				pageInfo = new PageInfo(docs.totalHits, page);
				for (int i = pageInfo.getStartRecord(); i < pageInfo.getEndRecord(); i++) {
					ScoreDoc scoreDoc = docs.scoreDocs[i];
					try {
						Document doc = searcher.doc(scoreDoc.doc);
						pageInfo.getData().add(createVoFromDocument(doc));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			} finally {
				try {
					nrtManager.release(searcher);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			logger.info("Search ... end<----");
			return pageInfo;
		}

		private String[] getVoPropertyNames() {
			Object source = BeanUtils.instantiate(domin.getVoClass());
			final BeanWrapper src = new BeanWrapperImpl(source);
			java.beans.PropertyDescriptor[] pds = src.getPropertyDescriptors();

			Set<String> emptyNames = new HashSet<String>();
			for (java.beans.PropertyDescriptor pd : pds) {
				emptyNames.add(pd.getName());
			}
			String[] result = new String[emptyNames.size()];
			return emptyNames.toArray(result);
		}

		private Document createDocumentFromVo(Object vo) {

			Document doc = new Document();
			String[] fieldset = getVoPropertyNames();
			for (String field : fieldset) {
				Object value = null;
				try {
					value = PropertyUtils.getSimpleProperty(vo, field);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				}
				String data = String.valueOf(value).trim();
				if ("id".equals(field)) {
					doc.add(new StringField(field, data, Store.YES));
				} else {
					if (value != null) {
						doc.add(new StringField(field, data, Store.YES));
					}
				}
			}
			return doc;
		}

		private Object createVoFromDocument(Document doc) {
			Object vo = BeanUtils.instantiate(domin.getVoClass());
			String[] props = getVoPropertyNames();
			for (String p : props) {
				if (StringUtils.isNotBlank(doc.get(p)) && !"class".equals(p)) {
					try {
						PropertyUtils.setSimpleProperty(vo, p, doc.get(p));
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					} catch (NoSuchMethodException e) {
						e.printStackTrace();
					}
				}
			}
			return vo;
		}

		private IndexSearcher getSeacher() {
			return nrtManager.acquire();
		}

		private void releaseSearcher(IndexSearcher searcher) {
			try {
				nrtManager.release(searcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void addVo(Object object) {
			logger.info("Create Document ... start");
			addDocument(createDocumentFromVo(object));
			commit();
			logger.info("Create Document ... end<----");
		}

		private void addVo(List list) {
			for (int i = 0; i < list.size(); i++) {
				addDocument(createDocumentFromVo(list.get(i)));
			}
			commit();
		}

		private void addDocument(Document doc) {
			try {
				writer.addDocument(doc);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void updateDocument(Term term, Document doc) {
			try {
				writer.updateDocument(term, doc);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void commit() {
			try {
				writer.commit();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
	}

}
