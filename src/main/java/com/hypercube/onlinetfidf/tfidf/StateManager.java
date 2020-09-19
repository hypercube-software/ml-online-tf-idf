package com.hypercube.onlinetfidf.tfidf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.hypercube.onlinetfidf.model.Document;
import com.hypercube.onlinetfidf.model.DocumentSibling;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.SparseVector;

@Repository
public class StateManager {

	private Logger logger = Logger.getLogger(StateManager.class.getName());

	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Autowired
	private Tokenizer tokenizer;

	public List<Document> updateState(Document document) {
		Long idDoc = createOrGetDocument(document);
		if (idDoc != null) {
			document.setId(idDoc);
			tokenizer.tokenize(document.getContent())
					.forEach(word -> this.updateStateFromWord(document, word));
		} else {
			logger.warning("Document ignored");
		}

		return computeTfIdf();
	}

	private void updateStateFromWord(Document doc, String word) {
		Long id = createOrGetWord(word);

		createOrUpdateCounter(doc.getId(), id);

	}

	private List<Document> computeTfIdf() {
		// get the number of vectors in the vector space
		Long documentCount = jdbcTemplate.queryForObject("SELECT count(*) from DOCUMENT", Long.class);

		// get the size of the vector space (how many dimensions)
		// we prefer MAX(ID) in case we remove elements for whatever reason
		// in this way the ID of a word is always a valid index in the vector
		// Long wordCount = jdbcTemplate.queryForObject("SELECT count(*) from WORD",
		// Long.class); // risky
		Long wordCount = jdbcTemplate.queryForObject("SELECT MAX(ID) from WORD", Long.class); // robust

		// here we hit the RAM if there are tons of documents, we could store the TFIDF
		// in the
		// database instead.
		List<Document> documents = jdbcTemplate.query("SELECT ID,NAME FROM DOCUMENT",
				(rs, rowNum) -> computeTF(rs.getLong(1), rs.getString(2), wordCount, documentCount));

		computeSiblings(documents);

		return documents;
	}

	private void computeSiblings(List<Document> documents) {
		Map<Long, Document> docMap = documents.stream()
				.collect(Collectors.toMap(Document::getId, Function.identity()));
		/*
		 * A correlation matrix is symmetric
		 * 
		 * so we should not compute n*n similarities, we do that:
		 * 
		 * x
		 * 
		 * x x
		 * 
		 * x x x
		 * 
		 * x x x x
		 * 
		 * x x x x x
		 * 
		 * since similarity(a,b) = similarity(b,a)
		 * 
		 * we compute n*(n-1)/2 similarities
		 */
		IntStream.range(0, documents.size())
				.forEach(docIdx1 -> {
					Document doc = documents.get(docIdx1);
					doc.setSiblings(IntStream.range(0, docIdx1)
							.peek(docIdx2 -> logger.info("compute similarity <" + docIdx1 + "," + docIdx2 + "> <"
									+ documents.get(docIdx1)
											.getTitle()
									+ "," + documents.get(docIdx2)
											.getTitle()
									+ ">"))
							.mapToObj(docIdx2 -> new DocumentSibling(documents.get(docIdx2)
									.getId(), computeSimilarity(doc, documents.get(docIdx2))))
							.filter(ds -> ds.getSimilarity() > 0)
							.peek(ds -> {
								// if Doc2 is a sibling of Doc1
								// we must also add Doc1 to the sibling of Doc2
								Document sibling = docMap.get(ds.getId());
								sibling.getSiblings()
										.add(new DocumentSibling(doc.getId(), ds.getSimilarity()));
							})
							.collect(Collectors.toList()));
				});
		logger.info("---------------------------");
		logger.info("Siblings for all documents:");
		documents.forEach(doc -> {
			if (doc.getSiblings()
					.size() == 0) {
				logger.info("\"" + doc.getTitle() + "\" sibling: none");
			} else {
				doc.setSiblings(doc.getSiblings()
						.stream()
						.sorted((s1, s2) -> s2.getSimilarity()
								.compareTo(s1.getSimilarity()))
						.collect(Collectors.toList()));

				doc.getSiblings()
						.forEach(sibling -> {
							logger.info("\"" + doc.getTitle() + "\" sibling: \"" + docMap.get(sibling.getId())
									.getTitle() + " similarity: " + sibling.getSimilarity());
						});
			}
		});
	}

	private Document computeTF(Long docId, String name, Long wordCount, Long documentCount) {
		// for a given document with id docId, we compute its vector
		SparseVector documentVector = new SparseVector(wordCount.intValue());

		// this query return how many words are used in the document
		Double docSize = jdbcTemplate.queryForObject("SELECT SUM(COUNT) FROM COUNTER WHERE DOCID=" + docId,
				Double.class);

		// this query found all words in the document with their count
		jdbcTemplate.query("SELECT WORDID, SUM(COUNT) FROM COUNTER WHERE DOCID=" + docId + " GROUP BY WORDID", rs -> {
			Long wordId = rs.getLong(1);
			Double normalizedTermFrequency = rs.getDouble(2) / docSize;
			Double idf = computeIDF(wordId, documentCount);
			int sparseVectorIndex = wordId.intValue() - 1;
			documentVector.set(sparseVectorIndex, normalizedTermFrequency * idf);
		});

		Document d = new Document();
		d.setId(docId);
		d.setTitle(name);
		d.setVector(documentVector);
		return d;
	}

	private Double computeSimilarity(Document d1, Document d2) {
		Double norm1 = d1.getVector()
				.norm(Vector.Norm.Two);
		Double norm2 = d2.getVector()
				.norm(Vector.Norm.Two);
		Double denom = norm1 * norm2;
		double cosineSimilarity = denom == 0 ? 0
				: d1.getVector()
						.dot(d2.getVector()) / denom;
		return cosineSimilarity;
	}

	private Double computeIDF(Long wordId, Long documentCount) {
		Long documentsWithThisWordCount = jdbcTemplate
				.queryForObject("SELECT count(*) from COUNTER WHERE WORDID = " + wordId, Long.class);
		Double idf = 1 + Math.log(documentCount.doubleValue() / documentsWithThisWordCount.doubleValue());
		return idf;
	}

	private Long createOrGetDocument(Document doc) {
		try (Connection connection = jdbcTemplate.getDataSource()
				.getConnection()) {
			PreparedStatement stmt = connection.prepareStatement("MERGE INTO DOCUMENT AS W " + "USING (SELECT '"
					+ doc.getTitle() + "' NEWNAME) AS S ON W.NAME=S.NEWNAME " + "WHEN NOT MATCHED THEN "
					+ "INSERT (NAME) VALUES(?) ", Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, doc.getTitle());
			int count = stmt.executeUpdate();
			if (count == 1) {
				try (ResultSet key = stmt.getGeneratedKeys()) {
					if (key.next()) {
						logger.info("New document: " + doc.getTitle());
						return key.getLong(1);
					}
				}
			} else {
				logger.info("Document already indexed:" + doc.getTitle());
				return null;
			}
		} catch (SQLException error) {
			logger.log(Level.SEVERE, "Unexpected error", error);
		}
		return null;
	}

	private Long createOrGetWord(String word) {
		try (Connection connection = jdbcTemplate.getDataSource()
				.getConnection()) {
			PreparedStatement stmt = connection
					.prepareStatement(
							"MERGE INTO WORD AS W " + "USING (SELECT '" + word + "' NEWNAME) AS S ON W.NAME=S.NEWNAME "
									+ "WHEN NOT MATCHED THEN " + "INSERT (NAME) VALUES(?) ",
							Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, word);
			int count = stmt.executeUpdate();
			if (count == 1) {
				try (ResultSet key = stmt.getGeneratedKeys()) {
					if (key.next()) {
						logger.info("New word: " + word);
						return key.getLong(1);
					}
				}
			} else {
				return jdbcTemplate.queryForObject("SELECT ID FROM WORD WHERE NAME=?", new Object[] { word },
						(rs, rowNum) -> rs.getLong(1));
			}
		} catch (SQLException error) {
			logger.log(Level.SEVERE, "Unexpected error", error);
		}
		return null;
	}

	private Long createOrUpdateCounter(Long docId, Long wordId) {
		try (Connection connection = jdbcTemplate.getDataSource()
				.getConnection()) {
			PreparedStatement stmt = connection.prepareStatement("MERGE INTO COUNTER AS W " + "USING (SELECT " + docId
					+ " NEWDOCID, " + wordId + " NEWWORDID) AS S " + "ON (W.DOCID=S.NEWDOCID AND W.WORDID=S.NEWWORDID) "
					+ "WHEN NOT MATCHED THEN " + "INSERT (DOCID,WORDID,COUNT) VALUES (S.NEWDOCID,S.NEWWORDID,1) "
					+ "WHEN MATCHED THEN " + "UPDATE SET COUNT = COUNT+1 ");
			stmt.executeUpdate();
		} catch (SQLException error) {
			logger.log(Level.SEVERE, "Unexpected error", error);
		}
		return null;
	}
}
