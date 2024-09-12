DO $$
    BEGIN

        IF NOT EXISTS(
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = 'analysis'
            )
        THEN
            EXECUTE 'ALTER SCHEMA public RENAME TO analysis';
            EXECUTE 'CREATE SCHEMA public';
        END IF;

    END
$$;