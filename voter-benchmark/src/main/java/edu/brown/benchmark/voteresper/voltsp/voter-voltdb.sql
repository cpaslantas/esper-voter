DROP PROCEDURE VoteSP IF EXISTS;
DROP PROCEDURE GenerateLeaderboardSP IF EXISTS;
DROP PROCEDURE DeleteContestantSP IF EXISTS;
DROP PROCEDURE InitializeSP IF EXISTS;

DROP VIEW v_votes_by_phone_number IF EXISTS;
DROP VIEW v_votes_by_contestant_number_state IF EXISTS;
DROP VIEW v_votes_by_contestant IF EXISTS;
DROP VIEW v_leaderboard IF EXISTS;
DROP TABLE contestants_tbl IF EXISTS;
DROP TABLE area_code_state IF EXISTS;
DROP TABLE votes_tbl IF EXISTS;
DROP TABLE proc_one_out IF EXISTS;
DROP TABLE w_rows IF EXISTS;
DROP TABLE w_staging IF EXISTS;
DROP TABLE leaderboard_tbl IF EXISTS;
DROP TABLE votes_count IF EXISTS;
DROP TABLE staging_count IF EXISTS;
DROP TABLE current_win_id IF EXISTS;

-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE contestants_tbl
(
  contestant_number integer     NOT NULL
, contestant_name   varchar(50) NOT NULL
, CONSTRAINT PK_contestants PRIMARY KEY
  (
    contestant_number
  )
);

PARTITION TABLE contestants_tbl ON COLUMN contestant_number;

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, CONSTRAINT PK_area_code_state PRIMARY KEY
  (
    area_code
  )
);
--PARTITION TABLE area_code_state ON COLUMN area_code;

-- votes table holds every valid vote.
--   voterdemohstores are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes_tbl
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL --REFERENCES contestants_tbl (contestant_number)
, created	     bigint  NOT NULL
--, CONSTRAINT PK_votes ASSUMEUNIQUE ( vote_id )
 --PARTITION BY ( phone_number )
);
CREATE ASSUMEUNIQUE INDEX IX_votes ON votes_tbl(vote_id);
PARTITION TABLE votes_tbl ON COLUMN phone_number;

CREATE TABLE w_staging
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL --REFERENCES contestants_tbl (contestant_number)
, created            timestamp  NOT NULL
, win_id             bigint     NOT NULL
-- PARTITION BY ( phone_number )
);

CREATE ASSUMEUNIQUE INDEX IX_w_staging ON w_staging(win_id);
PARTITION TABLE w_staging ON COLUMN contestant_number;

CREATE TABLE w_rows
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL --REFERENCES contestants_tbl (contestant_number)
, created            bigint  NOT NULL
, win_id             bigint     NOT NULL
--, stage_flag	     int        NOT NULL
--, CONSTRAINT PK_win PRIMARY KEY (win_id)
-- PARTITION BY ( phone_number )
);
CREATE ASSUMEUNIQUE INDEX IX_w_rows ON w_rows(win_id);
PARTITION TABLE w_rows ON COLUMN contestant_number;

--CREATE INDEX IX_stageflag ON w_rows(stage_flag);

CREATE TABLE leaderboard_tbl
(
  --phone_number       bigint    NOT NULL,
  contestant_number  integer   NOT NULL
, num_votes          integer
, CONSTRAINT PK_leaderboard PRIMARY KEY
  (
    contestant_number
  )
);

PARTITION TABLE leaderboard_tbl ON COLUMN contestant_number;

CREATE TABLE votes_count
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL
);

PARTITION TABLE votes_count ON COLUMN row_id;

CREATE TABLE staging_count
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL
);

PARTITION TABLE staging_count ON COLUMN row_id;

CREATE TABLE current_win_id
(
  row_id	     integer    NOT NULL,
  win_id     bigint    NOT NULL
);

PARTITION TABLE current_win_id ON COLUMN row_id;

-- rollup of votes by phone number, used to reject excessive voting
CREATE VIEW v_votes_by_phone_number
(
  phone_number
, num_votes
)
AS
   SELECT phone_number
        , COUNT(*)
     FROM votes_tbl
 GROUP BY phone_number
;

-- rollup of votes by contestant and state for the heat map and results
CREATE VIEW v_votes_by_contestant_number_state
(
  contestant_number
, state
, num_votes
)
AS
   SELECT contestant_number
        , state
        , COUNT(*)
     FROM votes_tbl
 GROUP BY contestant_number
        , state
;

CREATE VIEW v_votes_by_contestant
(
  contestant_number
, num_votes
)
AS
   SELECT contestant_number
        , COUNT(*)
     FROM votes_tbl
 GROUP BY contestant_number
;

--CREATE VIEW v_top_three_last_30_sec
--(
--  contestant_number, num_votes
--)
--AS
--   SELECT contestant_number
--        , count(*) 
--   FROM w_rows t
--   GROUP BY t.contestant_number
--;

 CREATE PROCEDURE from class VoteSP;
 CREATE PROCEDURE from class GenerateLeaderboardSP;
 CREATE PROCEDURE from class DeleteContestantSP;
 CREATE PROCEDURE from class InitializeSP;
