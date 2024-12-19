## Adding vote counts for EU votes
```sql
UPDATE votes
SET
    ForCount = subquery.for_count,
    AgainstCount = subquery.against_count
FROM (
    SELECT
        vote_id,
        SUM(CASE WHEN position = 'FOR' THEN 1 ELSE 0 END) AS for_count,
        SUM(CASE WHEN position = 'AGAINST' THEN 1 ELSE 0 END) AS against_count
    FROM
        member_votes
	WHERE member_votes.parliament = 'EU'
    GROUP BY
        vote_id
) AS subquery
WHERE
    votes.vote_id = subquery.vote_id AND
	votes.parliament = 'EU';
```

## Number of missing MPs per country
Used to check which MPs appear to have voted but are not in the *members_parliaments* table.
```sql
--- Checking missing MPs to have a membersVotes to membersParliaments foreign key
SELECT  parliament, COUNT(DISTINCT(MP_id)) as missing_mps
FROM member_votes
WHERE member_votes.MP_id NOT IN (SELECT id FROM members_parliaments)
GROUP BY parliament
```

## Stats of missing values in members_parliaments
This can be easily adapted to other tables.
```sql
--- Stats of missing values in the members_parliaments table
SELECT
	parliament,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id,
    SUM(CASE WHEN firstName IS NULL THEN 1 ELSE 0 END) AS firstName,
    SUM(CASE WHEN lastName IS NULL THEN 1 ELSE 0 END) AS lastName,
	SUM(CASE WHEN politicalGroup IS NULL THEN 1 ELSE 0 END) AS politicalGroup,
	SUM(CASE WHEN politicalGroup_short IS NULL THEN 1 ELSE 0 END) AS politicalGroup_short,
	SUM(CASE WHEN isActive IS "True" THEN 1 ELSE 0 END) AS isActive,
	SUM(CASE WHEN memberUntil IS NULL THEN 1 ELSE 0 END) AS memberUntil,
	SUM(CASE WHEN birthday IS NULL THEN 1 ELSE 0 END) AS birthday,
	COUNT(id) AS total
FROM
    members_parliaments
GROUP BY parliament;
```

## Active MPs incoherence
It happens that MPs are meant to have reached the end of their mandate but are still marked as *active* in the database. This query checks for this.
```sql
--- isActive but not member anymore ?
SELECT COUNT(*) as total
FROM members_parliaments
WHERE isActive = 'True' AND memberUntil < "2024-12-01" AND parliament='UK'
```

## Data analytics ?
Exemple involving the three databases : how did each group of French representatives vote when "Ukraine" was one of the keyword of the vote ?
```sql
--- How did french representatives vote when the resolution contained "Ukraine" as a keyword ?

SELECT
100*SUM(CASE WHEN position IS 'FOR' THEN 1 ELSE 0 END)/COUNT(*) AS "for",
100*SUM(CASE WHEN position IS 'AGAINST' THEN 1 ELSE 0 END)/COUNT(*) AS against,
100*SUM(CASE WHEN (position IS 'ABSTENTION') OR (position IS 'DID_NOT_VOTE') THEN 1 ELSE 0 END)/COUNT(*) AS NA,
members_parliaments.politicalGroup_short AS "group"
FROM member_votes
JOIN members_parliaments
ON members_parliaments.id = member_votes.MP_id
JOIN votes
ON votes.vote_id = member_votes.vote_id
WHERE members_parliaments.countryRepresentation = 'FR' AND (votes.keyword_0 = 'ukraine' OR votes.keyword_1 = 'ukraine' OR votes.keyword_2 = 'ukraine')
GROUP BY members_parliaments.politicalGroup_short
```