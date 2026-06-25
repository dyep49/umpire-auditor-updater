-- Midline (depth-midpoint) correctness support.
-- Adds: raw trajectory params, derived midline coords (px_mid/pz_mid), and the
-- preserved front-of-plate metric set. From 2026 on, the primary correct_call /
-- *_miss columns are computed at the midline; these *_front columns retain the
-- front-of-plate calculation for comparison.
--
-- Safe to re-run: every ADD COLUMN uses IF NOT EXISTS.

ALTER TABLE pitch
    ADD COLUMN IF NOT EXISTS traj_x0  double precision,
    ADD COLUMN IF NOT EXISTS traj_y0  double precision,
    ADD COLUMN IF NOT EXISTS traj_z0  double precision,
    ADD COLUMN IF NOT EXISTS traj_vx0 double precision,
    ADD COLUMN IF NOT EXISTS traj_vy0 double precision,
    ADD COLUMN IF NOT EXISTS traj_vz0 double precision,
    ADD COLUMN IF NOT EXISTS traj_ax  double precision,
    ADD COLUMN IF NOT EXISTS traj_ay  double precision,
    ADD COLUMN IF NOT EXISTS traj_az  double precision,
    ADD COLUMN IF NOT EXISTS px_mid   double precision,
    ADD COLUMN IF NOT EXISTS pz_mid   double precision,
    ADD COLUMN IF NOT EXISTS correct_call_front  boolean,
    ADD COLUMN IF NOT EXISTS x_miss_front        double precision,
    ADD COLUMN IF NOT EXISTS y_miss_front        double precision,
    ADD COLUMN IF NOT EXISTS total_miss_front    double precision,
    ADD COLUMN IF NOT EXISTS total_miss_in_front double precision;
