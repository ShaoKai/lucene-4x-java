package com.sky.lucene;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;

public class LuceneContextHolder {

	private static final Logger logger = LoggerFactory.getLogger(LuceneContextHolder.class);

	public static final Version LUCENE_VERSION = Version.LUCENE_40;

	public static final String INDEX_STORE_PATH = "";

	public static final String VALUE_OBJECT_PREFIX = "Vo";

	private static LuceneContextHolder instance;

	private Map<Domain, IndexManager> indexMap;

	private static final String EXCLUDE_ATTRIBUTES_PATTERN = "class";

	private static final String EXTRA_FULLTEXT_FIELD1 = "KEYWORD1";

	private static final String EXTRA_FULLTEXT_FIELD2 = "KEYWORD2";

	private LuceneContextHolder() {
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

	public synchronized static LuceneContextHolder getInstance() {
		if (instance == null) {
			instance = new LuceneContextHolder();
			instance.init();
		}
		return instance;
	}

	public enum Domain {
		DOCUMENT(VoDocument.class);

		private Class voClass;

		Domain(Class voClass) {
			this.voClass = voClass;
		}

		protected Class getVoClass() {
			return voClass;
		}
	}

	public class IndexManager {

		private Domain domin;

		private NRTManager nrtManager;

		private IndexWriter writer;

		private NRTManagerReopenThread reopenThread;

		public IndexManager(Domain domain) {
			this.domin = domain;
			File folder = new File(INDEX_STORE_PATH + domain.name());
			SimpleFSDirectory directory = null;
			try {
				directory = new SimpleFSDirectory(folder);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Directory directory = new RAMDirectory();
			// Writer
			try {
				IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, new SimpleAnalyzer(LUCENE_VERSION));
				config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
				// config.setMergePolicy(new LogByteSizeMergePolicy());
				writer = new IndexWriter(directory, config);
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				nrtManager = new NRTManager(new TrackingIndexWriter(writer), new SearcherFactory());
				// maxStaleSec ,minStaleSec
				// The maxStaleSec sets a lower bound on how frequently reopens should occur
				// The minStaleSec sets an upper bound on the time a user must wait before the search can run
				reopenThread = new NRTManagerReopenThread(nrtManager, 1.0, 0.025);
				reopenThread.setName("Lucene NRT " + domain.name() + " Reopen Thread");
				reopenThread.setDaemon(true);
				reopenThread.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public Query createKeywordQuery(String keyword) {
			// processing reserved words

			Set<String> fieldSet = this.getSearchPropertyNameSet();
			// fieldSet.add(EXTRA_FULLTEXT_FIELD1);
			// fieldSet.add(EXTRA_FULLTEXT_FIELD2);

			String[] fields = fieldSet.toArray(new String[fieldSet.size()]);

			BooleanQuery query = new BooleanQuery();
			for (int i = 0; i < fields.length; i++) {
				// WildcardQuery
			}

			return query;
		}

		public PageInfo search(Query query, int page, int pageSize) {
			return search(query, null, page, pageSize);
		}

		public PageInfo search(Query query, Sort sort, int page, int pageSize) {
			logger.debug("Search ... start");
			IndexSearcher searcher = null;
			PageInfo pageInfo = null;

			int MAX_RECORD_SIZE = 1000000000;
			try {
				searcher = nrtManager.acquire();
				TopDocs docs = null;
				if (sort == null) {
					docs = searcher.search(query, MAX_RECORD_SIZE);
				} else {
					docs = searcher.search(query, MAX_RECORD_SIZE, sort);
				}
				if (pageSize < 0) {
					pageSize = MAX_RECORD_SIZE;
				}

				pageInfo = new PageInfo(docs.totalHits, page, pageSize);
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
			} finally {
				try {
					nrtManager.release(searcher);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			logger.debug("Search ... end");
			return pageInfo;
		}

		/**
		 * @return All properties with lucene annotation of value object
		 */
		private Set<String> getSearchPropertyNameSet() {

			final Set<String> propertyNameSet = new HashSet<String>();

			ReflectionUtils.doWithFields(domin.getVoClass(), new FieldCallback() {

				public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
					if (field.getAnnotation(LuceneIgnore.class) == null) {
						propertyNameSet.add(field.getName());
					}
				}
			});
			return propertyNameSet;
		}

		/**
		 * @return All properties of value object(exclude class)
		 */
		private Set<String> getVoPropertyNameSet() {
			Set<String> propertyNameSet = new HashSet<String>();

			Object source = BeanUtils.instantiate(domin.getVoClass());
			final BeanWrapper src = new BeanWrapperImpl(source);
			java.beans.PropertyDescriptor[] pds = src.getPropertyDescriptors();

			for (java.beans.PropertyDescriptor pd : pds) {
				if (java.util.List.class.equals(pd.getPropertyType())) {
					continue;
				}
				if (pd.getName().matches(EXCLUDE_ATTRIBUTES_PATTERN)) {
					continue;
				}
				propertyNameSet.add(pd.getName());
			}
			return propertyNameSet;
		}

		private Document createDocumentFromVo(Object vo) {
			Document doc = new Document();

			Set<String> searchFieldSet = getSearchPropertyNameSet();
			Set<String> fieldSet = getVoPropertyNameSet();

			StringBuilder fullText = new StringBuilder();
			for (String field : fieldSet) {
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
				if (value == null) {
					continue;
				}
				String data = String.valueOf(value).trim();
				if (StringUtils.isBlank(data)) {
					continue;
				}
				doc.add(new StringField(field, data, Store.YES));

				if (searchFieldSet.contains(field)) {
					fullText.append(",").append(data);
				}
			}
			// EXTRA_FULLTEXT_FIELD1...
			// EXTRA_FULLTEXT_FIELD2...
			return doc;
		}

		private Object createVoFromDocument(Document doc) {
			// Reflection
			Object vo = BeanUtils.instantiate(domin.getVoClass());
			Set<String> fieldSet = getVoPropertyNameSet();
			for (String field : fieldSet) {
				try {
					Class targetType = PropertyUtils.getPropertyType(vo, field);

					if (StringUtils.isNotBlank(doc.get(field)) && !java.util.List.class.equals(targetType) && !field.matches(EXCLUDE_ATTRIBUTES_PATTERN)) {
						PropertyUtils.setSimpleProperty(vo, field, doc.get(field));
					}
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				}
			}
			// Must to copy the additional Id field in value object
			if (domin.getVoClass().getSimpleName().startsWith(VALUE_OBJECT_PREFIX)) {
				try {
					String idField = StringUtils.uncapitalize(domin.getVoClass().getSimpleName().replace(VALUE_OBJECT_PREFIX, "")) + "Id";
					PropertyUtils.setSimpleProperty(vo, "id", doc.get(idField));
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				}
			}
			return vo;
		}

		public void addVo(Object object) {
			logger.info("Create Document ... start");
			addDocument(createDocumentFromVo(object));
			commit();
			logger.info("Create Document ... end<----");
		}

		public void addVoList(List<?> list) {
			for (int i = 0; i < list.size(); i++) {
				addDocument(createDocumentFromVo(list.get(i)));
			}
			commit();
		}

		public void rebuild(List<?> list, ProgressListener progressListener) {
			progressListener.setProgressStart();
			try {
				writer.deleteAll();
			} catch (IOException e) {
				e.printStackTrace();
			}
			for (int i = 0; i < list.size(); i++) {
				if ((i % 20) == 0) {
					progressListener.setProgressValue(Math.round(((float) i / (float) list.size()) * 100));
				}
				addDocument(createDocumentFromVo(list.get(i)));
			}
			commit();
			progressListener.setProgressDone();
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
				// writer.deleteDocuments(term);
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

		public void updateVo(Object vo) {
			VoObject myVo = (VoObject) vo;
			updateDocument(new Term("id", myVo.getId()), createDocumentFromVo(vo));
			commit();
		}
	}

	public class LpadSortComparatorSource extends FieldComparatorSource {

		public LpadSortComparatorSource() {
		}

		@Override
		public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {

			return new FieldComparator() {

				@Override
				public int compare(int slot1, int slot2) {
					return 0;
				}

				@Override
				public void setBottom(int slot) {

				}

				@Override
				public int compareBottom(int doc) throws IOException {
					return 0;
				}

				@Override
				public void copy(int slot, int doc) throws IOException {

				}

				@Override
				public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
					return null;
				}

				@Override
				public Object value(int slot) {
					return null;
				}

				@Override
				public int compareDocToValue(int doc, Object value) throws IOException {
					return 0;
				}
			};
		}
	}

}
