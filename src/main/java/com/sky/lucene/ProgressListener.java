package com.sky.lucene;

import java.io.Serializable;
import java.util.UUID;

public class ProgressListener implements Serializable {

	private int progressValue = 0;

	private long elapsedTime = 0;

	private long startTime = 0;

	private String errorMessage = null;

	private long taskId;

	public ProgressListener() {
		taskId = UUID.randomUUID().getMostSignificantBits();
	}

	public long getTaskId() {
		return taskId;
	}

	public int getProgressValue() {
		return progressValue;
	}

	void setProgressValue(int progressValue) {
		// TODO 1~100
		this.progressValue = progressValue;
	}

	public boolean isFailed() {
		return errorMessage != null;
	}

	void setProgressStart() {
		progressValue = 0;
		startTime = System.nanoTime();
	}

	void setProgressDone() {
		if (!isFailed()) {
			this.elapsedTime = System.nanoTime() - startTime;
			progressValue = 100;
		}
	}

	public boolean getProgressDone() {
		return progressValue == 100;
	}

	public boolean getProgressStarted() {
		return progressValue < 0;
	}

	public long getElapsedTime() {
		if (progressValue < 100) {
			return System.nanoTime() - startTime;
		} else {
			return this.elapsedTime;
		}
	}

	void setElapsedTime(long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
		progressValue = -1;
	}

}
