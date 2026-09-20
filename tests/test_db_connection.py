from src.utils.database import get_connection


def test_database_connection():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_database(),
                    current_user,
                    version();
                """
            )

            database, user, version = cur.fetchone()

            print(f"Database : {database}")
            print(f"User     : {user}")
            print(f"Version  : {version}")

            assert database is not None
            assert user is not None


if __name__ == "__main__":
    test_database_connection()