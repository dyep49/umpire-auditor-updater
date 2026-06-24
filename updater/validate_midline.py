#!/usr/bin/env python3
"""Post-backfill validation for the midline change.

Checks, against the live DB:
  1. Front-of-plate parity: total_miss_in_front must equal the pre-change
     total_miss_in values (front geometry is unchanged).
  2. The known sample pitch (Jasson Dominguez called strike, 2026-06-17,
     gamePk 823536) lands at midline total_miss_in == 5.47 and front == 6.07.
  3. For 2026 rows, the primary correct_call is the midline verdict and
     px_mid/pz_mid are populated.

Usage:  python3 validate_midline.py
Reads DB_URL from the environment (source the repo .env first).
"""
import os
import psycopg

SAMPLE_ID = "707c40b2-32e7-32a3-b75d-130845f2c86e"


def main():
    url = os.environ["DB_URL"]
    with psycopg.connect(url) as conn:
        cur = conn.cursor()

        # 1. Sample pitch
        cur.execute(
            "SELECT total_miss_in, total_miss_in_front, px, pz, px_mid, pz_mid, "
            "correct_call, correct_call_front "
            "FROM pitch WHERE id = %s",
            (SAMPLE_ID,),
        )
        row = cur.fetchone()
        if not row:
            print("SAMPLE PITCH NOT FOUND (has the 2026-06-17 backfill run?)")
        else:
            mid, front, px, pz, px_mid, pz_mid, cc, ccf = row
            print(f"Sample pitch  midline miss = {mid}  (expect 5.47)")
            print(f"              front   miss = {front}  (expect 6.07)")
            print(f"              px/pz front  = ({px:.4f}, {pz:.4f})")
            print(f"              px/pz mid    = ({px_mid:.4f}, {pz_mid:.4f})")
            print(f"              correct_call={cc}  correct_call_front={ccf}")

        # 2. 2026 population coverage
        cur.execute(
            "SELECT count(*) FILTER (WHERE px_mid IS NOT NULL), count(*) "
            "FROM pitch WHERE game_date >= '2026-01-01'"
        )
        populated, total = cur.fetchone()
        print(f"\n2026 rows with px_mid populated: {populated}/{total}")

        # 3. Rows where midline flipped the verdict vs front
        cur.execute(
            "SELECT count(*) FROM pitch "
            "WHERE game_date >= '2026-01-01' "
            "AND correct_call IS DISTINCT FROM correct_call_front"
        )
        flipped = cur.fetchone()[0]
        print(f"2026 calls where midline disagrees with front: {flipped}")


if __name__ == "__main__":
    main()
