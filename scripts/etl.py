import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from files into staging tables;
    Parameter: cur, a cursor object used to execute commands;
    Parameter: conn, a database connection object."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables;
    Parameter: cur, a cursor object used to execute commands;
    Parameter: coon, a database connection oject."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read configuration information to establish connection
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create a connection object to the AWS Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data from files
    load_staging_tables(cur, conn)
    # Insert data from staging files
    insert_tables(cur, conn)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()