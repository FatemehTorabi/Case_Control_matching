-----------------------------------------------------------
--DacVap: 	Vaccine Safety
--BY:		fatemeh.torabi@swansea.ac.uk
--DT: 		2021-04-08
--aim:		match cases to controls
-----------------------------------------------------------

---------------------------------------------------------------
-- stage 1: get all elgible controls
---------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW0911V.DACVAP_CC_STAGE1');

CREATE TABLE sailw0911v.dacvap_cc_stage1 (
	case_alf             BIGINT NOT NULL,
	control_alf          BIGINT NOT NULL,
	incident_dt          DATE,
	sex                  SMALLINT,
	age_match            VARCHAR(7),
	msoa2011_cd          VARCHAR(10),
	random               DOUBLE,
	rand_row_seq         INTEGER,
	PRIMARY KEY (CASE_ALF, CONTROL_ALF)
) DISTRIBUTE BY HASH (CASE_ALF, CONTROL_ALF);

--granting access to team mates
GRANT ALL ON TABLE SAILW0911V.DACVAP_CC_STAGE1 TO ROLE NRDASAIL_SAIL_0911_ANALYST;

INSERT INTO SAILW0911V.DACVAP_CC_STAGE1
(
	case_alf,
	control_alf,
	incident_dt,
	sex,
	age_match,
	msoa2011_cd
)
SELECT
	a.alf_e AS case_alf,
	b.alf_e AS control_alf,
	a.incident_dt,
	b.sex,
	b.age_match,
	b.msoa2011_cd
FROM
( --CASE TABLE
	SELECT DISTINCT
		alf_e,
		c20_gndr_cd AS sex,
		age,
		CASE
			WHEN age < 80 THEN CAST(age AS VARCHAR(7))
			WHEN age BETWEEN  80 AND  81 THEN '80.81'
			WHEN age BETWEEN  82 AND  83 THEN '82.83'
			WHEN age BETWEEN  84 AND  85 THEN '84.85'
			WHEN age BETWEEN  86 AND  87 THEN '86.87'
			WHEN age BETWEEN  88 AND  89 THEN '88.89'
			WHEN age BETWEEN  90 AND  94 THEN '90.94'
			WHEN age BETWEEN  95 AND  99 THEN '95.99'
			WHEN age BETWEEN 100 AND 104 THEN '100.104'
			WHEN age BETWEEN 105 AND 110 THEN '105.110'
		END AS age_match,
		msoa2011_cd,
		incident_dt
	FROM
		sailw0911v.dacvap_cohort
	WHERE
		is_sample = 1
		AND	clearance_incident_dt IS NULL
		AND incident_event = 1
)	AS A
INNER JOIN
( -- CONTROL TABLE
	SELECT DISTINCT
		alf_e,
		c20_gndr_cd AS sex,
		age,
		CASE
			WHEN age < 80 THEN CAST(age AS VARCHAR(7))
			WHEN age BETWEEN  80 AND  81 THEN '80.81'
			WHEN age BETWEEN  82 AND  83 THEN '82.83'
			WHEN age BETWEEN  84 AND  85 THEN '84.85'
			WHEN age BETWEEN  86 AND  87 THEN '86.87'
			WHEN age BETWEEN  88 AND  89 THEN '88.89'
			WHEN age BETWEEN  90 AND  94 THEN '90.94'
			WHEN age BETWEEN  95 AND  99 THEN '95.99'
			WHEN age BETWEEN 100 AND 104 THEN '100.104'
			WHEN age BETWEEN 105 AND 110 THEN '105.110'
		END AS age_match,
		msoa2011_cd,
		incident_dt
	FROM
		sailw0911v.dacvap_cohort
	WHERE
		is_sample = 1
		AND	clearance_incident_dt IS NULL
)	AS B
ON
	a.alf_e != b.alf_e
	AND a.sex = b.sex
	AND	a.age_match = b.age_match
	AND	a.msoa2011_cd = b.msoa2011_cd
	AND	(a.incident_dt < b.incident_dt OR b.incident_dt IS NULL)
;

UPDATE SAILW0911V.DACVAP_CC_STAGE1
SET
	RANDOM = RAND(),
	RAND_ROW_SEQ = ROW_NUMBER() OVER(PARTITION BY case_alf ORDER BY RANDOM);


--SELECT count(DISTINCT case_alf) ALFS_WITH_LESS_THAN_10_CONT FROM (
--				SELECT DISTINCT case_alf, max(rand_row_seq) CONT_NUM
--				FROM SAILW0911V.DACVAP_CC_STAGE1
--				GROUP BY case_alf
--				)
--WHERE cont_num <= 10;
-------------------------------------------
----CHECKS
--
--SELECT count(DISTINCT case_alf) case_alf, count(*) all_rows FROM SAILW0911V.DACVAP_CC_STAGE1;
--
----WHO DIDN'T MATCHED
--SELECT
--	DISTINCT alf_e, C20_GNDR_CD AS SEX, AGE,
--	CASE
--			WHEN AGE < 80 THEN AGE
--			WHEN AGE BETWEEN 80 AND 81 THEN 80.81
--			WHEN AGE BETWEEN 82 AND 83 THEN 82.83
--			WHEN AGE BETWEEN 84 AND 85 THEN 84.85
--			WHEN AGE BETWEEN 86 AND 87 THEN 86.87
--			WHEN AGE BETWEEN 88 AND 89 THEN 88.89
--			WHEN AGE BETWEEN 90 AND 94 THEN 90.94
--			WHEN AGE BETWEEN 95 AND 99 THEN 95.99
--			WHEN AGE BETWEEN 100 AND 104 THEN 100.104
--			WHEN AGE BETWEEN 105 AND 110 THEN 105.110
--			END AS AGE_MATCH ,
--	MSOA2011_CD ,INCIDENT_DT
--FROM
--	SAILW0911V.DACVAP_COHORT
--WHERE
--IS_SAMPLE=1
--AND
--INCIDENT_EVENT=1
--AND
--CLEARANCE_INCIDENT_DT IS NULL
--AND
--alf_e NOT IN (SELECT DISTINCT case_alf FROM SAILW0911V.DACVAP_CC_STAGE1);

---------------------------------------------------------------
-- stage 2: rows are cases and 10 randomly picked controls
---------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW0911V.DACVAP_CC');

CREATE TABLE SAILW0911V.DACVAP_CC (
	alf_e        BIGINT NOT NULL,
	groups       INTEGER NOT NULL,
	alf_type     VARCHAR(7),
	incident_dt  DATE,
	sex          SMALLINT,
	age_match    VARCHAR(7),
	msoa2011_cd  VARCHAR(10),
	PRIMARY KEY (alf_e, groups)
);

--granting access to team mates
GRANT ALL ON TABLE SAILW0911V.DACVAP_CC TO ROLE NRDASAIL_SAIL_0911_ANALYST;

INSERT INTO SAILW0911V.DACVAP_CC
WITH
	t_case AS (
		SELECT DISTINCT
			case_alf AS alf_e,
			DENSE_RANK() OVER(ORDER BY case_alf) AS groups,
			'CASE' AS alf_type,
			incident_dt,
			sex,
			age_match,
			msoa2011_cd
		FROM
		(
			SELECT *
			FROM SAILW0911V.DACVAP_CC_STAGE1
			WHERE RAND_ROW_SEQ BETWEEN 0 AND 10
		)
	),
	t_control AS (
		SELECT DISTINCT
			control_alf AS alf_e,
			DENSE_RANK() OVER(ORDER BY case_alf) AS groups,
			'CONTROL' AS alf_type,
			incident_dt,
			sex,
			age_match,
			msoa2011_cd
		FROM
		(
			SELECT *
			FROM SAILW0911V.DACVAP_CC_STAGE1
			WHERE RAND_ROW_SEQ BETWEEN 0 AND 10
		)
	)
SELECT * FROM t_case
UNION
SELECT * FROM t_control;

DROP TABLE SAILW0911V.DACVAP_CC_STAGE1;

--Q/A
--SELECT DISTINCT c Numbers_in_matched_group, count(*) Total_groups
--FROM 	(
--		SELECT DISTINCT GROUPS , count(*) c
--		FROM SAILW0911V.DACVAP_CC
--		GROUP BY GROUPS
--		)
--GROUP BY c
--ORDER BY 2;
