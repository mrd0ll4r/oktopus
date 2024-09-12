DO $$
    BEGIN

        IF EXISTS(
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = 'analysis'
            )
        THEN
            EXECUTE 'DROP SCHEMA public';
            EXECUTE 'ALTER SCHEMA analysis RENAME TO public';
        END IF;

    END
$$;