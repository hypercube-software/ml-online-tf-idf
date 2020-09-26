package com.hypercube.onlinetfidf.model;

import java.util.ArrayList;
import java.util.List;

import no.uib.cipr.matrix.sparse.SparseVector;

public class Document {
	private Long id;
	private String title;
	private String content;
	private SparseVector vector;
	private List<DocumentSibling> siblings = List.of();
	
	public Document() {
		
	}
	
	public Document(Long id, String title) {
		super();
		this.id = id;
		this.title = title;
	}
	public SparseVector getVector() {
		return vector;
	}
	public void setVector(SparseVector vector) {
		this.vector = vector;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public List<DocumentSibling> getSiblings() {
		return siblings;
	}
	public void setSiblings(List<DocumentSibling> siblings) {
		this.siblings = siblings;
	}
}
