-- Functions to create and draw histograms with PostgreSQL.
--
-- psql# WITH email_lengths AS (
--    -#    SELECT length(email) AS length
--    -#    FROM auth_user
--    -# )
--    -# SELECT * FROM show_histogram((SELECT histogram(length, 12, 32, 10) FROM email_lengths))
--  bucket |  range  | count | overflow |                 bar                  |             cumbar             | cumsum |      cumpct
-- --------+---------+-------+----------+--------------------------------------+--------------------------------+--------+-------------------
--       0 | [12,14) |    17 |       -4 | =======--                            | ==                             |     21 |             0.056
--       1 | [14,16) |    83 |        0 | ==================================== | ========                       |    104 | 0.277333333333333
--       2 | [16,18) |    18 |        0 | ========                             | ==========                     |    122 | 0.325333333333333
--       3 | [18,20) |    34 |        0 | ===============                      | ============                   |    156 |             0.416
--       4 | [20,22) |    46 |        0 | ====================                 | ================               |    202 | 0.538666666666667
--       5 | [22,24) |    44 |        0 | ===================                  | ====================           |    246 |             0.656
--       6 | [24,26) |    61 |        0 | ==========================           | =========================      |    307 | 0.818666666666667
--       7 | [26,28) |    26 |        0 | ===========                          | ===========================    |    333 |             0.888
--       8 | [28,30) |    13 |        0 | ======                               | ============================   |    346 | 0.922666666666667
--       9 | [30,32) |    11 |       18 | =====++++++++                        | ============================== |    375 |                 1
-- (10 rows)
-- psql#
BEGIN;

CREATE OR REPLACE FUNCTION histogram_version ()
    RETURNS text
    AS $$
    SELECT
        '0.2.0'::text;

$$
LANGUAGE SQL
IMMUTABLE PARALLEL SAFE;

DROP TYPE IF EXISTS histogram_result CASCADE;

CREATE TYPE histogram_result AS (
    count integer,
    overflow integer,
    total integer,
    bucket integer,
    RANGE numrange
);

CREATE OR REPLACE FUNCTION histogram_sfunc (
    state histogram_result[],
    val float8,
    min float8,
    max float8,
    nbuckets integer
)
    RETURNS histogram_result[]
    AS $$
DECLARE
    bucket integer;
    overflow integer;
    incr integer;
    width float8;
    i integer;
    init_range numrange;
BEGIN
    -- Initialize the state
    IF state[0] IS NULL THEN
        width := (max - min) / nbuckets;
        FOR i IN
        SELECT
            *
        FROM
            generate_series(0, nbuckets - 1)
            LOOP
                init_range := numrange((min + i * width)::numeric, (min + (i + 1) * width)::numeric);
                state[i] := (0,
                    0,
                    0,
                    i,
                    init_range);
            END LOOP;
    END IF;
    bucket := floor(((val - min) / (max - min)) * nbuckets);
    bucket := GREATEST (bucket, 0);
    bucket := LEAST (bucket, nbuckets - 1);
    overflow := CASE WHEN val < min THEN
        - 1
    WHEN val >= max THEN
        1
    ELSE
        0
    END;
    incr := CASE WHEN overflow = 0 THEN
        1
    ELSE
        0
    END;
    state[bucket] = (state[bucket].count + incr,
        state[bucket].overflow + overflow,
        state[bucket].total + 1,
        state[bucket].bucket,
        state[bucket].RANGE);
    RETURN state;
END;
$$
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION histogram_combinefunc (
    state_left histogram_result[],
    state_right histogram_result[]
)
    RETURNS histogram_result[]
    AS $$
DECLARE
    i integer;
BEGIN
    -- left or right might not be initialized yet
    -- in that case return the other size
    IF state_left[0] IS NULL THEN
        RETURN state_right;
    END IF;
    IF state_right[0] IS NULL THEN
        RETURN state_left;
    END IF;
    FOR i IN array_lower(state_left, 1)..array_upper(state_left, 1)
    LOOP
        state_left[i] = (state_left[i].count + state_right[i].count,
            state_left[i].overflow + state_right[i].overflow,
            state_left[i].total + state_right[i].total,
            state_left[i].bucket,
            state_left[i].RANGE);
    END LOOP;
    RETURN state_left;
END;
$$
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE histogram (float8, float8, float8, integer) (
    SFUNC = histogram_sfunc,
    COMBINEFUNC = histogram_combinefunc,
    STYPE = histogram_result[],
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION histogram_bar (
    v float8,
    tick_size float8,
    overflow float8 = 0
)
    RETURNS text
    AS $$
DECLARE
    suffix text;
BEGIN
    suffix := CASE WHEN overflow < 0 THEN
        repeat('-', (- overflow * tick_size)::integer)
    ELSE
        ''
    END;
    suffix := suffix || CASE WHEN overflow > 0 THEN
        repeat('+', (overflow * tick_size)::integer)
    ELSE
        ''
    END;
    RETURN repeat('=', (v * tick_size)::integer) || suffix;
END;
$$
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION show_histogram (
    h histogram_result[]
)
    RETURNS TABLE (
            bucket integer,
            RANGE numrange,
            count integer,
            overflow integer,
            bar text,
            cumbar text,
            cumsum integer,
            cumpct float8
        )
        AS $$
DECLARE
    r histogram_result;
    min_count integer := (
        SELECT
            min(x.total)
        FROM
            unnest(h) AS x);
    max_count integer := (
        SELECT
            max(x.total)
        FROM
            unnest(h) AS x);
    total_count integer := (
        SELECT
            sum(x.total)
        FROM
            unnest(h) AS x);
    bar_max_width integer := 30;
    bar_tick_size float8 := bar_max_width / max_count::float8;
    bar text;
    cumsum integer := 0;
    cumpct float8;
BEGIN
    FOREACH r IN ARRAY h LOOP
        IF r.bucket IS NULL THEN
            CONTINUE;
        END IF;
        cumsum := cumsum + r.count + abs(r.overflow);
        cumpct := (cumsum::float8 / total_count);
        bar := histogram_bar (r.count, bar_tick_size, r.overflow);
        RETURN QUERY
    VALUES (r.bucket,
        r.range,
        r.count,
        r.overflow,
        bar,
        histogram_bar (cumpct, bar_max_width),
        cumsum,
        cumpct);
    END LOOP;
END;
$$
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE ROWS 50;

COMMIT;

