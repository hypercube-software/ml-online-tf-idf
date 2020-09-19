package com.hypercube.onlinetfidf.model;

public class Word {
	private Long id;
	private String name;
	private Long count;
	private Double Idf;
	
	public Double getIdf() {
		return Idf;
	}
	public void setIdf(Double idf) {
		Idf = idf;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
}
