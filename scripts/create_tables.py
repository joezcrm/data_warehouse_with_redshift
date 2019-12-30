import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables before recreating them;
    Parameter: cur, a cursor object used to execute commands;
    Parameter: conn, a database connection object."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables that are needed;
    Parameter: cur, a cursor object used to execute commands;
    Parameter: conn, a database connection object."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Obtain configuration information
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Get a connection object to AWS Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop tables before creating
    drop_tables(cur, conn)
    # Create tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()