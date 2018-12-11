# CS411-csvDatabase system

Our project supports ad-hoc SQL-based queries over data files in the CSV format. It supports one time selection from one or more tables. It will be command line based. Users need to put the corresponding csv files under the same folder with python files. 

### Environment Configuration:

IDE: PyCharm

Compiler: Anaconda with Python 2.7

Libraries: Pandas, numpy, sqlparse, egenix-mx-base


### Usage: 

Run commendLine.py. Users can build index or query through it.

Build Index: index (followed by csv filenames)

Eg. Index review.csv business.csv photos.csv

### Query:

Enter SQL languages with the correct format.

#### SQL Format: 

SELECT (attribute names) FROM (csv filenames) WHERE (conditions) ON (join conditions) (DISTINCT)
(write DISTINCT at the very end to remove duplicates)

#### Example:

SELECT A.Year, A.Film, A.Name FROM movies.csv M1, movies.csv M2 WHERE  M.imdb_score = (3.1 + A.Winner)*2 AND (M.language like 'S%' OR A.Winner = 1) ON M1.director_name = M2.director_name, M1.director_name = M3.director_name DISTINCT

SELECT B.name, R1.user_id, R2.user_id FROM business.csv B, r.csv R1, r.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND B.city = 'Champaign' ON (B.business_id = R1.business_id, B.business_id = R2.business_id) DISTINCT

##### The detail of the implementation can be found in csvDatabase report.pdf
