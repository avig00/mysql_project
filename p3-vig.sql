-- Amogh Vig
-- amogh.vig@vanderbilt.edu
-- Project Part 3

-- CREATE DATABASE
DROP DATABASE IF EXISTS project_part_3;
CREATE DATABASE IF NOT EXISTS project_part_3;
USE project_part_3;

-- CREATE MEGATABLE
DROP TABLE IF EXISTS voters;
CREATE TABLE IF NOT EXISTS voters (
precinct_desc VARCHAR(7),
party_cd VARCHAR(255), 
race_code VARCHAR(255), 
ethnic_code VARCHAR(255), 
sex_code VARCHAR(255), 
age INT, 
pct_portion VARCHAR(30), 
first_name VARCHAR(255), 
middle_name VARCHAR(255), 
last_name VARCHAR(255), 
name_suffix_lbl VARCHAR(255), 
full_name_mail VARCHAR(255), 
mail_addr1 VARCHAR(50), 
mail_addr2 VARCHAR(80), 
mail_city_state_zip VARCHAR(50), 
house_num INT, 
street_dir VARCHAR(50), 
street_name VARCHAR(30), 
street_type_cd VARCHAR(10), 
street_sufx_cd VARCHAR(10), 
unit_num VARCHAR(10), 
res_city_desc VARCHAR(100), 
state_cd VARCHAR(2), 
zip_code INT, 
registr_dt DATETIME, 
voter_reg_num BIGINT(20), 
status_cd VARCHAR(255), 
municipality_desc VARCHAR(100), 
ward_desc VARCHAR(100), 
cong_dist_desc VARCHAR(100), 
super_court_desc VARCHAR(100), 
nc_senate_desc VARCHAR(100), 
nc_house_desc VARCHAR(100), 
county_commiss_desc VARCHAR(100), 
school_dist_desc VARCHAR(100), 
E1 INT, 
E1_date DATE, 
E1_VotingMethod VARCHAR(1),
E1_PartyCd VARCHAR(10), 
E2 INT, 
E2_Date DATE, 
E2_VotingMethod VARCHAR(1), 
E2_PartyCd VARCHAR(10), 
E3 INT, 
E3_Date DATE, 
E3_VotingMethod VARCHAR(1), 
E3_PartyCd VARCHAR(10), 
E4 INT, 
E4_Date DATE, 
E4_VotingMethod VARCHAR(1), 
E4_PartyCd VARCHAR(10), 
E5 INT, 
E5_Date DATE, 
E5_VotingMethod VARCHAR(1), 
E5_PartyCd VARCHAR(10)
) ENGINE = innoDB;

-- Load data
LOAD DATA INFILE '/Users/vikasvig/Downloads/voterDataF22.csv' INTO TABLE voters
FIELDS TERMINATED BY ';' ENCLOSED BY '$'
LINES TERMINATED BY '\r\n'

(precinct_desc, party_cd, race_code, ethnic_code, sex_code, age, pct_portion, first_name, middle_name, last_name, name_suffix_lbl, 
full_name_mail, mail_addr1, mail_addr2, mail_city_state_zip, house_num, street_dir, street_name, street_type_cd, street_sufx_cd, unit_num, 
res_city_desc, state_cd, zip_code, registr_dt, voter_reg_num, status_cd, municipality_desc, ward_desc, cong_dist_desc, super_court_desc, 
nc_senate_desc, nc_house_desc, county_commiss_desc, school_dist_desc, E1, @E1_date, E1_VotingMethod, E1_PartyCd, E2, @E2_Date, 
E2_VotingMethod, E2_PartyCd, E3, @E3_Date, E3_VotingMethod, E3_PartyCd, E4, @E4_Date, E4_VotingMethod, E4_PartyCd, E5, 
@E5_Date, E5_VotingMethod, E5_PartyCd)

SET E1_Date = STR_TO_DATE(CASE WHEN @E1_Date = '' THEN NULL ELSE @E1_Date END, '%m/%d/%Y'),
	E2_Date = STR_TO_DATE(CASE WHEN @E2_Date = '' THEN NULL ELSE @E2_Date END, '%m/%d/%Y'),
	E3_Date = STR_TO_DATE(CASE WHEN @E3_Date = '' THEN NULL ELSE @E3_Date END, '%m/%d/%Y'),
	E4_Date = STR_TO_DATE(CASE WHEN @E4_Date = '' THEN NULL ELSE @E4_Date END, '%m/%d/%Y'),
	E5_Date = STR_TO_DATE(CASE WHEN @E5_Date = '' THEN NULL ELSE @E5_Date END, '%m/%d/%Y');
    


-- CREATE TABLES

-- TABLE 1
-- Create person table
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
voter_reg_num BIGINT(20) PRIMARY KEY,
party_cd VARCHAR(255),
race_code VARCHAR(255),
ethnic_code VARCHAR(255),
sex_code VARCHAR(255),
age TINYINT(3), 
first_name VARCHAR(255), 
middle_name VARCHAR(255), 
last_name VARCHAR(255), 
full_name_mail VARCHAR(255),
name_suffix_lbl VARCHAR(255),
registr_dt DATETIME,
status_cd VARCHAR(255)
);

-- Insert into person table
INSERT IGNORE INTO person (voter_reg_num, party_cd, race_code, ethnic_code, sex_code, age, first_name,
                           middle_name, last_name, full_name_mail, name_suffix_lbl, registr_dt, status_cd)
SELECT voter_reg_num, 
	   party_cd, 
       race_code, 
       ethnic_code, 
       sex_code, 
       age, 
       first_name, 
       middle_name, 
       last_name, 
	   full_name_mail, 
       name_suffix_lbl, 
       registr_dt, 
       status_cd
FROM voters;

-- TABLE 2
-- Create city_state table
DROP TABLE IF EXISTS city_state;
CREATE TABLE IF NOT EXISTS city_state (
res_city_desc VARCHAR(45) PRIMARY KEY,
state_cd VARCHAR(45)
);

-- Insert into city_state table
INSERT IGNORE INTO city_state (res_city_desc, state_cd)
SELECT res_city_desc, state_cd
FROM voters;


-- TABLE 3
-- Create zip_city table
DROP TABLE IF EXISTS zip_city;
CREATE TABLE IF NOT EXISTS zip_city (
zip_code CHAR(5) PRIMARY KEY,
res_city_desc VARCHAR(45),
CONSTRAINT fk1 FOREIGN KEY (res_city_desc)
			REFERENCES city_state(res_city_desc)
            ON DELETE CASCADE
);

-- Insert into zip_city table
INSERT IGNORE INTO zip_city (zip_code, res_city_desc)
SELECT zip_code, res_city_desc
FROM voters;


-- TABLE 4
-- Create election table
DROP TABLE IF EXISTS election;
CREATE TABLE IF NOT EXISTS election (
election_ID TINYINT(4) PRIMARY KEY,
election_Name CHAR(2),
election_Date DATE
);

-- Insert into election table
INSERT IGNORE INTO election
SELECT E1, "E1" AS election_Name, E1_Date
FROM voters
UNION ALL
SELECT E2, 'E2' AS election_Name, E2_Date
FROM voters
UNION ALL
SELECT E3, 'E3' AS election_Name, E3_Date
FROM voters
UNION ALL
SELECT E4, 'E4' AS election_Name, E4_Date
FROM voters
UNION ALL
SELECT E5, 'E5' AS election_Name, E5_Date
FROM voters;

-- TABLE 5
-- Create voting_history table
DROP TABLE IF EXISTS voting_history;
CREATE TABLE IF NOT EXISTS voting_history (
voter_reg_num BIGINT(20),
election_ID TINYINT(4),
voting_method CHAR(1),
party_cd CHAR(3),
PRIMARY KEY (voter_reg_num, election_ID)
);

-- Insert into voting_history table
INSERT IGNORE INTO voting_history
SELECT voter_reg_num, E1, E1_VotingMethod AS voting_method, E1_PartyCd AS party_cd
FROM voters
WHERE E1_PartyCd IS NOT NULL AND E1_PartyCd <> ''
UNION ALL
SELECT voter_reg_num, E2, E2_VotingMethod AS voting_method, E2_PartyCd AS party_cd
FROM voters
WHERE E2_PartyCd IS NOT NULL AND E2_PartyCd <> ''
UNION ALL
SELECT voter_reg_num, E3, E3_VotingMethod AS voting_method, E3_PartyCd AS party_cd
FROM voters
WHERE E3_PartyCd IS NOT NULL AND E3_PartyCd <> ''
UNION ALL
SELECT voter_reg_num, E4, E4_VotingMethod AS voting_method, E4_PartyCd AS party_cd
FROM voters
WHERE E4_PartyCd IS NOT NULL AND E4_PartyCd <> ''
UNION ALL
SELECT voter_reg_num, E5, E5_VotingMethod AS voting_method, E5_PartyCd AS party_cd
FROM voters
WHERE E5_PartyCd IS NOT NULL AND E5_PartyCd <> '';

-- TABLE 6
-- Create area table
DROP TABLE IF EXISTS area;
CREATE TABLE area (
pct_portion VARCHAR(50),
precinct_desc VARCHAR(255),
ward_desc VARCHAR(255),
cong_dist_desc VARCHAR(255),
super_court_desc VARCHAR(255),
nc_senate_desc VARCHAR(255),
nc_house_desc VARCHAR(255),
county_commiss_desc VARCHAR(255),
school_dist_desc VARCHAR(255),
PRIMARY KEY(pct_portion)
);

-- Insert into area table
INSERT IGNORE INTO area (pct_portion, precinct_desc, ward_desc, cong_dist_desc, super_court_desc,
                         nc_senate_desc, nc_house_desc, county_commiss_desc, school_dist_desc)
SELECT pct_portion, 
	   precinct_desc, 
       ward_desc, 
       cong_dist_desc, 
       super_court_desc,
       nc_senate_desc, 
       nc_house_desc, 
       county_commiss_desc, 
       school_dist_desc
FROM voters;

-- TABLE 7
-- Create residential_address_info table
DROP TABLE IF EXISTS residential_address_info;
CREATE TABLE residential_address_info (
voter_reg_num BIGINT(20),
house_num VARCHAR(45),
street_dir VARCHAR(45),
street_name VARCHAR(45),
street_type_cd VARCHAR(45),
street_sufx_cd VARCHAR(45),
unit_num VARCHAR(45),
zip_code CHAR(5),
pct_portion VARCHAR(25),
PRIMARY KEY(voter_reg_num),
CONSTRAINT fk2 FOREIGN KEY (zip_code)
			REFERENCES zip_city(zip_code)
            ON DELETE CASCADE,
CONSTRAINT fk3 FOREIGN KEY (pct_portion)
			REFERENCES area(pct_portion)
            ON DELETE CASCADE
);

-- Insert into residential_address_info 
INSERT IGNORE INTO residential_address_info (voter_reg_num, house_num, street_dir, street_name, 
											 street_type_cd, street_sufx_cd, unit_num, zip_code, pct_portion)
SELECT voter_reg_num,
	   house_num,
       street_dir,
       street_name,
       street_type_cd,
       street_sufx_cd,
       unit_num,
       zip_code,
       pct_portion
FROM voters;


-- ADD FUNCTIONALITIES

-- FUNCTIONALITY 1: SEARCH VOTER/VOTING HISTORY
-- Query for voter name
-- Testing search voter name for voter_reg_num = 529
SELECT full_name_mail
FROM person 
WHERE voter_reg_num = 529;
-- Returns HELEN M ALEXANDER

-- Testing search voter name for voter_reg_num = 789
SELECT full_name_mail 
FROM person 
WHERE voter_reg_num = 789;
-- Returns JEANETTE E ALEXANDER

-- Procedure for voting history
DROP PROCEDURE IF EXISTS get_voting_record;

DELIMITER //

CREATE PROCEDURE get_voting_record (
	IN voter_num INT)
    
BEGIN
	SELECT election_ID, voting_method, party_cd
    FROM voting_history
    WHERE voter_reg_num = voter_num;

END //

DELIMITER ;

-- Testing search voting history for voter_reg_num = 529
CALL get_voting_record(529);
-- Returns 3 rows

-- Testing search voting history for voter_reg_num = 789
CALL get_voting_record(789);
-- Returns 1 row

-- FUNCTIONALITY 2: REGISTER VOTER

-- Create insert_audit table
DROP TABLE IF EXISTS insert_audit;
CREATE TABLE insert_audit (
voter_reg_num INT,  
party_cd VARCHAR(255),
first_name VARCHAR(255), 
middle_name VARCHAR(255), 
last_name VARCHAR(255), 
name_suffix_lbl VARCHAR(255), 
full_name_mail VARCHAR(255),
insert_time DATETIME
) ENGINE = innoDB;

-- Create party_audit table
DROP TABLE IF EXISTS party_audit;
CREATE TABLE party_audit (
voter_reg_num INT,  
party_cd VARCHAR(255),
insert_time DATETIME
) ENGINE = innoDB;

-- Trigger check for party input
DROP TRIGGER IF EXISTS check_insert_voter;
DELIMITER // 
CREATE TRIGGER check_insert_voter
BEFORE INSERT ON person
FOR EACH ROW 
BEGIN 
-- unknown party cd 
	IF NEW.party_cd <> 'DEM' 
		AND NEW.party_cd <> 'UNA' 
        AND NEW.party_cd <> 'REP' 
        AND NEW.party_cd <> 'LIB' 
        AND NEW.party_cd <> 'CST' 
        AND NEW.party_cd <> 'GRE' THEN 
        -- insert the data into miscellaneous table 
		INSERT INTO party_audit (voter_reg_num, party_cd, insert_time) 
		VALUES (NEW.voter_reg_num, NEW.party_cd, NOW());
        -- change party cd to N/A
        SET NEW.party_cd = 'N/A';
        
	END IF;
END //
DELIMITER ;

-- insertion procedure 
DROP PROCEDURE IF EXISTS insert_voter;

DELIMITER // 
CREATE PROCEDURE insert_voter(
	IN voter_reg_num CHAR(12), 
    IN first_name VARCHAR(45), 
    IN middle_name VARCHAR(45), 
    IN last_name VARCHAR(45), 
    IN name_suffix_lbl VARCHAR(45), 
    IN party_cd CHAR(3),
    OUT msg VARCHAR(45))
BEGIN 
	DECLARE sql_error INT DEFAULT FALSE;
	DECLARE CONTINUE HANDLER FOR 1062 SET sql_error = TRUE;
    -- SELECT 'Duplicate key error encountered' INTO msg;
    -- DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SELECT 'SQL exception encountered' INTO msg;
    
    START TRANSACTION;
    
    INSERT INTO person (voter_reg_num, party_cd, first_name, middle_name, last_name, name_suffix_lbl, full_name_mail)
    VALUES (voter_reg_num, party_cd, first_name, middle_name, last_name, name_suffix_lbl, 
    CONCAT(first_name,' ', middle_name,' ', last_name, ' ', name_suffix_lbl));
    
    -- INSERT INTO demographic_data(voter_reg_num, party_cd)
    -- VALUES (voter_reg_num, party_cd);
	IF sql_error= FALSE THEN
		INSERT INTO residential_address_info (voter_reg_num)
        VALUES (voter_reg_num);
		COMMIT;
        SELECT 'INSERTED SUCCESSFULLY' INTO msg;
    ELSE
		ROLLBACK;
		SELECT 'Duplicate key error encountered' INTO msg;
	END IF;
    SELECT msg;
END //
DELIMITER ;

DROP TRIGGER IF EXISTS audit_insert_voter ;
DELIMITER //
CREATE TRIGGER audit_insert_voter 
AFTER INSERT ON person
FOR EACH ROW
BEGIN
    INSERT INTO insert_audit (voter_reg_num, party_cd, full_name_mail, insert_time)
    VALUES (NEW.voter_reg_num, NEW.party_cd, NEW.full_name_mail, NOW());
END //
DELIMITER ;

-- Testing insert_voter
CALL insert_voter(222, 'SAM', '', 'ALTMAN', '', 'BJP', @msg);
CALL insert_voter(333, 'ELON', '', 'MUSK', '', 'BJP', @msg);
CALL insert_voter(444, 'STEVE', '', 'JOBS', '', 'BJP', @msg);
CALL insert_voter(999, 'BILL', '', 'GATES', '', 'BJP', @msg);
-- Inserts successfully

-- Checking rows of insert_audit table
SELECT *
FROM
insert_audit;
-- Returns 4 rows

-- Checking rows of party_audit table
SELECT *
FROM
party_audit;
-- Returns 4 rows

-- Checking duplicate call
CALL insert_voter(222, 'SAM', '', 'ALTMAN', '', 'BJP', @msg);
-- Outputs message: Duplicate key error encountered

-- Checking rows of insert_audit table
SELECT *
FROM
insert_audit;
-- Still returns 4 rows (nothing was inserted)

-- Checking rows of party_audit table
SELECT *
FROM
party_audit;
-- -- Still returns 4 rows (nothing was inserted)

-- FUNCTIONALITY 3: DELETE VOTER

-- Create delete_audit table
DROP TABLE IF EXISTS delete_audit;
CREATE TABLE delete_audit (
party_cd VARCHAR(255), 
race_code VARCHAR(255), 
ethnic_code VARCHAR(255), 
sex_code VARCHAR(255), 
age TINYINT, 
first_name VARCHAR(255), 
middle_name VARCHAR(255), 
last_name VARCHAR(255), 
name_suffix_lbl VARCHAR(255), 
full_name_mail VARCHAR(255), 
house_num VARCHAR(45), 
street_dir VARCHAR(45), 
street_name VARCHAR(45), 
street_type_cd VARCHAR(45), 
street_sufx_cd VARCHAR(45), 
unit_num VARCHAR(45), 
zip_code CHAR(5), 
registr_dt DATE, 
voter_reg_num INT, 
status_cd VARCHAR(2), 
election_ID TINYINT(4), 
voting_method CHAR(1),
deleted_time DATETIME
) ENGINE = innoDB;

DROP PROCEDURE IF EXISTS delete_voter;

DELIMITER //
CREATE PROCEDURE delete_voter (
    IN voter_num INT)
    
BEGIN
  DECLARE voter_found INT DEFAULT FALSE;
  DECLARE sql_error INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR 1062 SET sql_error = TRUE;

  -- Check if Voter registration number exists in the table
  SELECT COUNT(*) INTO voter_found FROM person WHERE voter_reg_num = voter_num;

  IF voter_found = 0 THEN
    SELECT 'Voter does not exist' AS message;
  ELSE
    -- Begin transaction
    
    -- Insert deleted voter information into audit table
    INSERT INTO delete_audit (party_cd, race_code, ethnic_code, sex_code, age, first_name, middle_name, last_name, name_suffix_lbl, 
                          full_name_mail, house_num, street_dir, street_name, street_type_cd, street_sufx_cd, unit_num, zip_code, 
                          registr_dt, voter_reg_num, status_cd, election_ID, voting_method , deleted_time)
    SELECT p.party_cd, 
        race_code, 
        ethnic_code, 
        sex_code, 
        age, 
        first_name, 
        middle_name, 
        last_name, 
        name_suffix_lbl, 
        full_name_mail, 
        house_num, 
        street_dir, 
        street_name, 
        street_type_cd, 
        street_sufx_cd, 
        unit_num, 
        zip_code, 
        registr_dt, 
        voter_reg_num, 
        status_cd, 
        election_ID, 
        voting_method, 
        NOW()
    FROM person p
        INNER JOIN residential_address_info r USING(voter_reg_num)
        INNER JOIN voting_history v USING(voter_reg_num)
    WHERE voter_reg_num = voter_num;
  
    -- Delete voter information
    DELETE FROM person 
    WHERE voter_reg_num = voter_num;
    DELETE FROM residential_address_info 
    WHERE voter_reg_num = voter_num;
    DELETE FROM voting_history 
    WHERE voter_reg_num = voter_num;
    
    SELECT CONCAT('Voter with registration number ', voter_num, ' has been deleted') AS message;
  END IF;
  
END //
DELIMITER ;

-- Testing delete voter for voter_reg_num = 529
CALL delete_voter(529);
-- Returns 1 row with message confirming deletion

-- Testing delete voter for voter_reg_num = 789
CALL delete_voter(789);
-- Returns 1 row with message confirming deletion

-- Checking delete_audit table
SELECT *
FROM delete_audit;
-- Returns 4 rows

-- Checking deleting a voter that does not exist
CALL delete_voter(777);
-- Outputs message: Voter does not exist

-- Checking delete_audit table
SELECT *
FROM delete_audit;
-- Still returns 4 rows (nothing was deleted)

-- FUNCTIONALITY 4: ANALYTICS 1

-- Get Constituents Stats
DROP VIEW IF EXISTS constituent_stats;
CREATE VIEW constituent_stats AS
SELECT party_cd, COUNT(*), ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person)), 4) AS 'percentage'
FROM person
GROUP BY party_cd
ORDER BY COUNT(*) DESC;

-- Checking constituent_stats view
SELECT *
FROM constituent_stats;
-- Returns 7 rows

-- Get Dem Regional Stats
DROP VIEW IF EXISTS dem_region_stats;
CREATE VIEW dem_region_stats AS
SELECT res_city_desc, COUNT(*), 
		ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM city_state
												JOIN zip_city USING(res_city_desc)
												JOIN residential_address_info USING(zip_code)
												JOIN person USING(voter_reg_num)
                                                WHERE party_cd = 'DEM')), 4) AS 'percentage'
FROM city_state
	JOIN zip_city USING(res_city_desc)
    JOIN residential_address_info USING(zip_code)
    JOIN person USING(voter_reg_num)
 WHERE party_cd = 'DEM'
 GROUP BY res_city_desc
 ORDER BY COUNT(*) DESC;
 
-- Checking dem_region_stats view
SELECT *
FROM dem_region_stats;
-- Returns 6 rows

-- Get Dem Gender Stats
DROP VIEW IF EXISTS dem_gender_stats;
CREATE VIEW dem_gender_stats AS
SELECT sex_code, COUNT(sex_code), 
				 ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person WHERE party_cd = 'DEM')), 4) AS 'percentage'
FROM person 
WHERE party_cd = 'DEM'
GROUP BY sex_code
ORDER BY COUNT(sex_code) DESC;

-- Checking dem_gender_stats view
SELECT *
FROM dem_gender_stats;
-- Returns 3 rows


-- Get Number of Switchers
DROP PROCEDURE IF EXISTS switched_election;

DELIMITER // 
CREATE PROCEDURE switched_election(
	IN party CHAR(3), 
    IN elect_num INT)

BEGIN
	SELECT num_switchers
FROM
	(SELECT p.party_cd, election_Name, COUNT(*) AS 'num_switchers'
	FROM voting_history AS v
		JOIN person AS p USING(voter_reg_num)
		JOIN election AS e USING(election_ID)
    WHERE p.party_cd <> v.party_cd
    GROUP BY p.party_cd, election_Name) AS tmp
WHERE election_Name = CONCAT('E', elect_num)
AND party_cd = party;

END //

DELIMITER ; 

-- Testing switched_election for GRE party and election 5
CALL switched_election('GRE', 5);
-- Returns 13


-- FUNCTIONALITY 5: ANALYTICS 2

-- Get Age Summary Stats
-- This view shows descritpive statistics of the ages of voters affiliated with each party
DROP VIEW IF EXISTS age_summary_stats;
CREATE VIEW age_summary_stats AS
SELECT party_cd,
	   MIN(age) AS min_age,
       ROUND(AVG(age), 2) AS mean_age,
       MAX(age) AS max_age,
       ROUND(STD(age), 2) AS sd_age
FROM person
WHERE party_cd <> 'N/A'
GROUP BY party_cd
ORDER BY mean_age DESC;

-- Checking age_summary_stats view
SELECT *
FROM age_summary_stats;
-- Returns 6 rows

-- Get Race Stats
DROP VIEW IF EXISTS race_stats;
CREATE VIEW race_stats AS
SELECT race_code, party_cd, ROUND((COUNT(voter_reg_num) * 100.0 / (SELECT COUNT(voter_reg_num) FROM person)), 4) AS 'percentage'
FROM person 
GROUP BY race_code, party_cd
ORDER BY percentage DESC;

-- Checking race_summary_stats view
SELECT *
FROM race_stats;
-- returns 39 rows