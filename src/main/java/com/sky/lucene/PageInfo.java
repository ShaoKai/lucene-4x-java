package com.sky.lucene;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageInfo<V> {
	private static final Logger logger = LoggerFactory.getLogger(PageInfo.class);
	private List<V> data = new ArrayList<V>();
	private int recordCount;
	private int pageSize = 100;
	private int page;
	private int totalPage;
	private int startRecord;
	private int endRecord;

	public PageInfo(int recordCount, int page) {
		super();
		this.totalPage = recordCount % pageSize == 0 ? recordCount / pageSize : recordCount / pageSize + 1;

		this.recordCount = recordCount;
		this.page = page > totalPage ? totalPage : page;

		// logger.info("page,totalPage : {},{} ", this.page, totalPage);

		if (recordCount == 0) {
			this.startRecord = 0;
			this.endRecord = 0;
		} else {
			int start = (this.page - 1) * this.pageSize;
			int end = start + this.pageSize;
			end = end > this.recordCount ? this.recordCount : end;

			this.startRecord = start;
			this.endRecord = end;
		}
		// logger.info("startRecord,endRecord : {},{} ", this.startRecord,
		// this.endRecord);
	}

	public int getStartRecord() {
		return startRecord;
	}

	public int getEndRecord() {
		return endRecord;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(int totalPage) {
		this.totalPage = totalPage;
	}

	public int getPage() {
		return page;
	}

	public void setPage(int page) {
		this.page = page;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public List<V> getData() {
		return data;
	}

	public void setData(List<V> data) {
		this.data = data;
	}

}
