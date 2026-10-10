from pathlib import Path

import psycopg2


class DatabaseClient:
    def __init__(self, config):
        self.config = config

    def connect(self):
        return psycopg2.connect(
            host=self.config.postgres_host,
            port=self.config.postgres_port,
            dbname=self.config.postgres_database,
            user=self.config.postgres_user,
            password=self.config.postgres_password,
        )

    def execute_script(self, filename):
        script_path = Path(self.config.sql_dir) / filename

        if not script_path.exists():
            raise FileNotFoundError(
                f"SQL script not found: {script_path}"
            )

        sql = script_path.read_text(
            encoding="utf-8"
        )

        if not sql.strip():
            raise ValueError(
                f"SQL script is empty: {script_path}"
            )

        connection = self.connect()

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        print(f"Executed {filename}.")
        