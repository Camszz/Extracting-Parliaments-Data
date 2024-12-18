-- SQLite
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
