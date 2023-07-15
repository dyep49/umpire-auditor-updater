CREATE TABLE "pitch" (
  "id" varchar UNIQUE,
  "game_date" date,
  "play_description" varchar,
  "home_team" varchar,
  "away_team" varchar,
  "home_team_id" int,
  "away_team_id" int,
  "inning" int,
  "inning_half" varchar,
  "outs" int,
  "sz_top" float,
  "sz_bottom" float,
  "px" float,
  "pz" float,
  "code" varchar,
  "strikes" int,
  "balls" int,
  "umpire_name" varchar,
  "game_id" int,
  "datetime_start" timestamptz,
  "timestamp_start_home" time(2),
  "timestamp_start_away" time(2),
  "start_seconds_home" int,
  "start_seconds_away" int,
  "timestamp_end_home" time(2),
  "timestamp_end_away" time(2),
  "is_call" bool,
  "correct_call" bool,
  "blown_strikeout" bool,
  "blown_walk" bool,
  "x_miss" float,
  "y_miss" float,
  "total_miss" float,
  "total_miss_in" float,
  "home_away_benefit" varchar,
  "team_benefit" varchar,
  "team_benefit_id" int,
  "team_hurt" varchar,
  "team_hurt_id" int,
  "player_type_benefit" varchar,
  "batter_id" int,
  "pitcher_id" int,
  "catcher_id" int,
  "umpire_id" int,
  "possible_bad_data" bool,
  "created_at" timestamptz,
  "updated_at" timestamptz
);

CREATE INDEX datetime_start_index
on pitch (datetime_start);

CREATE TABLE "umpire" (
  "id" int UNIQUE NOT NULL,
  "name" varchar
);

CREATE TABLE "team" (
  "id" int UNIQUE NOT NULL,
  "name" varchar,
  "abbreviation" varchar
);

CREATE TABLE "game" (
  "id" int UNIQUE NOT NULL,
  "home_team" varchar,
  "away_team" varchar,
  "game_date" date,
  "game_type" varchar,
  "correct_calls" int,
  "incorrect_calls" int,
  "total_calls" int,
  "calls_benefit_home" int,
  "calls_benefit_away" int,
  "correct_call_rate" float,
  "umpire_name" varchar,
  "umpire_id" int,
  "home_team_id" int,
  "away_team_id" int
);

CREATE TABLE "player" (
  "id" int UNIQUE NOT NULL,
  "name" varchar
);

CREATE TABLE "ejection" (
  "id" varchar UNIQUE NOT NULL,
  "game_date" date,
  "game_id" int,
  "description" varchar,
  "home_team" varchar,
  "away_team" varchar,
  "home_team_id" int,
  "away_team_id" int,
  "umpire_name" varchar,
  "timestamp_start_home" time(2),
  "timestamp_start_away" time(2),
  "start_seconds_home" int,
  "start_seconds_away" int,
  "player_id" int,
  "umpire_id" int
);

ALTER TABLE "pitch" ADD FOREIGN KEY ("home_team_id") REFERENCES "team" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("away_team_id") REFERENCES "team" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("game_id") REFERENCES "game" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("team_benefit_id") REFERENCES "team" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("team_hurt_id") REFERENCES "team" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("batter_id") REFERENCES "players" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("pitcher_id") REFERENCES "players" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("catcher_id") REFERENCES "players" ("id");

ALTER TABLE "pitch" ADD FOREIGN KEY ("umpire_id") REFERENCES "umpire" ("id");

ALTER TABLE "game" ADD FOREIGN KEY ("umpire_id") REFERENCES "umpire" ("id");

ALTER TABLE "game" ADD FOREIGN KEY ("home_team_id") REFERENCES "team" ("id");

ALTER TABLE "game" ADD FOREIGN KEY ("away_team_id") REFERENCES "team" ("id");

ALTER TABLE "ejections" ADD FOREIGN KEY ("game_id") REFERENCES "game" ("id");

ALTER TABLE "ejections" ADD FOREIGN KEY ("home_team_id") REFERENCES "team" ("id");

ALTER TABLE "ejections" ADD FOREIGN KEY ("away_team_id") REFERENCES "team" ("id");

ALTER TABLE "ejections" ADD FOREIGN KEY ("umpire_id") REFERENCES "umpire" ("id");

CREATE or REPLACE FUNCTION notify_new_ejection()
    RETURNS trigger
     LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT') then
 
        perform pg_notify('new_ejection_added', 
        json_build_object(
             'description', NEW.description,
             'home_timestamp', NEW.timestamp_start_home,
			       'away_timestamp', NEW.timestamp_start_away,
						 'start_seconds_home', NEW.start_seconds_home,
						 'start_seconds_away', NEW.start_seconds_away
           )::text);
    end if;
 
    return null;
end
$BODY$;

CREATE TRIGGER after_insert_ejection
    AFTER INSERT
    ON ejections
    FOR EACH ROW
    EXECUTE PROCEDURE notify_new_ejection();

CREATE ROLE readaccess;
GRANT CONNECT ON DATABASE umpire_auditor_prod;
GRANT USAGE ON SCHEMA public TO readaccess;
