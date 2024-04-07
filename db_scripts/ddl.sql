DROP TABLE IF EXISTS cric_scores.tbl_schedule;
CREATE TABLE `cric_scores`.`tbl_schedule` (
  `team1` VARCHAR(50) NOT NULL,
  `team2` VARCHAR(50) NOT NULL,
  `match_number` INT NOT NULL,
  `stadium` VARCHAR(100) NOT NULL,
  `city` VARCHAR(20) NOT NULL,
  `winner` VARCHAR(50) DEFAULT NULL,
  `link` VARCHAR(200) NOT NULL,
  `match_DATETIME` DATETIME NOT NULL,
  `match_date` DATE NOT NULL,
  `match_time` TIME NOT NULL,
  `timestamp` bigINT NOT NULL,
  PRIMARY KEY (`match_number`)
)

DROP TABLE IF EXISTS cric_scores.tbl_result;
CREATE TABLE `cric_scores`.tbl_result` (
  `match_number` INT NOT NULL,
  `team1` VARCHAR(5) NOT NULL,
  `team1_runs` INT NOT NULL,
  `team1_wickets` INT NOT NULL,
  `team1_overs` float NOT NULL,
  `team2` VARCHAR(5) NOT NULL,
  `team2_runs` INT NOT NULL,
  `team2_wickets` INT NOT NULL,
  `team2_overs` float NOT NULL,
  `winner` VARCHAR(5) NOT NULL,
  `won_by_number` INT NOT NULL,
  `won_by_metric` VARCHAR(10) NOT NULL,
  `potm` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`match_number`)
)

DROP TABLE IF EXISTS cric_scores.tbl_batting;
CREATE TABLE `cric_scores`.`tbl_batting` (
  `match_number` INT NOT NULL,
  `inning` INT NOT NULL,
  `player_id` INT NOT NULL,
  `batsman_name` VARCHAR(50) NOT NULL,
  `batsman_link` VARCHAR(100) NOT NULL,
  `batting_position` INT NOT NULL, 
  `dismissal_type` VARCHAR(20) NOT NULL,
  `dismissal_bowler` VARCHAR(50) NULL,
  `dismissal_caught` VARCHAR(50) NULL,
  `dismissal_stump` VARCHAR(50) NULL,
  `dismissal_run_out` VARCHAR(100) NULL,
  `batsman_runs` INT NOT NULL,
  `batsman_balls` INT NOT NULL,
  `batsman_fours` INT NOT NULL,
  `batsman_sixes` INT NOT NULL,
  `batsman_strike_rate` FLOAT NOT NULL,
  PRIMARY KEY (`match_number`, `inning`, `player_id`));
  
DROP TABLE IF EXISTS cric_scores.tbl_bowling;
CREATE TABLE `cric_scores`.`tbl_bowling` (
  `match_number` INT NOT NULL,
  `inning` INT NOT NULL,
  `player_id` INT NOT NULL,
  `bowler_name` VARCHAR(50) NOT NULL,
  `bowler_link` VARCHAR(100) NOT NULL,
  `bowling_position` INT NOT NULL,
  `bowler_overs` FLOAT NOT NULL,
  `bowler_maiden` INT NOT NULL,
  `bowler_runs` INT NOT NULL,
  `bowler_wickets` INT NOT NULL,
  `bowler_nb` INT NOT NULL,
  `bowler_wb` INT NOT NULL,
  `bowler_economy` FLOAT NOT NULL,
  PRIMARY KEY (`match_number`, `inning`, `player_id`));


DROP TABLE IF EXISTS cric_scores.tbl_extras;
CREATE TABLE `cric_scores`.`tbl_extras` (
  `match_number` INT NOT NULL,
  `inning` INT NOT NULL,
  `extras_total` INT NOT NULL,
  `extras_b` INT NOT NULL,
  `extras_lb` INT NOT NULL,
  `extras_w` INT NOT NULL,
  `extras_nb` INT NOT NULL,
  `extras_p` INT NOT NULL,
  PRIMARY KEY (`match_number`, `inning`));