#!/usr/bin/env python3
"""Post-backfill validation for the midline change.

Checks, against the live DB:
  1. The known sample pitch (Jasson Dominguez called strike, 2026-06-17,
     gamePk 823536) lands at midline total_miss_in == 5.47 and front == 6.07.
  2. 2026 px_mid population is essentially complete (untracked games aside).
  3. Some 2026 calls actually flip between front and midline (the change has
     an effect), and the flip rate is sane (not ~everything, which would mean
     correct_call_front is NULL for un-backfilled rows).

Usage:  python3 validate_midline.py
Reads DB_URL from the environment (source the repo .env first).

Exits non-zero if any check fails, so it can gate a backfill in CI / scripts.
"""
import os
import sys

import psycopg

SAMPLE_ID = "707c40b2-32e7-32a3-b75d-130845f2c86e"
EXPECT_MID = 5.47
EXPECT_FRONT = 6.07
# Tracked-game coverage should be ~complete; allow a little slack for games
# that genuinely lost Statcast tracking (no pX at all) and live games.
MIN_POPULATION_RATE = 0.99
# A definitional change to the plate plane should flip a small but non-zero
# fraction of calls. Bracket it to catch "nothing happened" and "everything
# flipped" (the latter means correct_call_front is NULL for un-backfilled rows).
MIN_FLIP_RATE = 0.005
MAX_FLIP_RATE = 0.20


def main():
    failures = []

    def check(ok, msg):
        print(("  ok  " if ok else "FAIL  ") + msg)
        if not ok:
            failures.append(msg)

    url = os.environ["DB_URL"]
    with psycopg.connect(url) as conn:
        cur = conn.cursor()

        # 1. Sample pitch
        cur.execute(
            "SELECT total_miss_in, total_miss_in_front, px_mid, pz_mid "
            "FROM pitch WHERE id = %s",
            (SAMPLE_ID,),
        )
        row = cur.fetchone()
        if not row:
            check(False, "sample pitch not found (has the 2026-06-17 backfill run?)")
        else:
            mid, front, px_mid, pz_mid = row
            check(mid == EXPECT_MID, f"sample midline miss = {mid} (expect {EXPECT_MID})")
            check(front == EXPECT_FRONT, f"sample front miss = {front} (expect {EXPECT_FRONT})")
            check(px_mid is not None and pz_mid is not None,
                  f"sample px_mid/pz_mid populated ({px_mid}, {pz_mid})")

        # 2. 2026 population coverage
        cur.execute(
            "SELECT count(*) FILTER (WHERE px_mid IS NOT NULL), count(*) "
            "FROM pitch WHERE game_date >= '2026-01-01'"
        )
        populated, total = cur.fetchone()
        rate = populated / total if total else 0
        check(rate >= MIN_POPULATION_RATE,
              f"2026 px_mid population {populated}/{total} = {rate:.4f} "
              f"(min {MIN_POPULATION_RATE})")

        # 3. Flip rate, counting only fully-backfilled rows so un-scored
        #    (NULL correct_call_front) rows don't masquerade as disagreements.
        cur.execute(
            "SELECT count(*) FILTER (WHERE correct_call IS DISTINCT FROM correct_call_front), "
            "count(*) "
            "FROM pitch WHERE game_date >= '2026-01-01' AND correct_call_front IS NOT NULL"
        )
        flips, scored = cur.fetchone()
        flip_rate = flips / scored if scored else 0
        check(MIN_FLIP_RATE <= flip_rate <= MAX_FLIP_RATE,
              f"2026 front-vs-midline flips {flips}/{scored} = {flip_rate:.4f} "
              f"(expect {MIN_FLIP_RATE}..{MAX_FLIP_RATE})")

    print()
    if failures:
        print(f"VALIDATION FAILED: {len(failures)} check(s) failed")
        sys.exit(1)
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()
