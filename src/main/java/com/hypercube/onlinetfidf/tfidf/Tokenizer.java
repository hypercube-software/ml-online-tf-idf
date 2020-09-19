package com.hypercube.onlinetfidf.tfidf;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.stereotype.Component;

@Component
public class Tokenizer {
	public Stream<String> tokenize(String content) {
		content = content.replaceAll("[”“’•—…|@$\\/#°\\-:&*+=\\[\\]?!(){},''\\\">_<;%\\\\.]", " ");
		return Arrays.asList(content.split("\\s"))
				.stream()
				.filter(Predicate.not(String::isEmpty))
				.map(w -> w.toLowerCase());
	}
}
