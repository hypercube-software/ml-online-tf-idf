package com.hypercube.onlinetfidf.model;

public class DocumentSibling {
	private Long id;
	private Double similarity;
	public DocumentSibling(Long id, Double similarity) {
		super();
		this.id = id;
		this.similarity = similarity;
	}
	public Long getId() {
		return id;
	}
	public Double getSimilarity() {
		return similarity;
	}
}
