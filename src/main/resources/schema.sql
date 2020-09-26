CREATE TABLE IF NOT EXISTS DOCUMENT  (
	  id INT AUTO_INCREMENT  PRIMARY KEY,
	  name VARCHAR(250) NOT NULL,
	  unique(name)
	);

CREATE TABLE IF NOT EXISTS WORD  (
	  id INT AUTO_INCREMENT  PRIMARY KEY,
	  name VARCHAR(250) NOT NULL,
	  unique(name)
	);
	
CREATE TABLE IF NOT EXISTS COUNTER  (
	  docId INT NOT NULL,
	  wordId INT NOT NULL,
	  count INT NOT NULL,
	  primary key (docId,wordId),
	  foreign key (docId) references DOCUMENT(ID),
	  foreign key (wordId) references WORD(ID)
	);

    
	