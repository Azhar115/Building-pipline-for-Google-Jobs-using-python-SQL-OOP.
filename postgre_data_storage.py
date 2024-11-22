import config
def connection_postgre_checking():
    try:
        # Establish connection to PostgreSQL
        conn_ps = config.config_postgre()
        print("PostgreSQL connection is established")

        # Example query for PostgreSQL (You can execute this if needed)
        query_ps = 'SELECT * FROM public."check"'
        # Perform operations with conn_ps here (e.g., execute query, fetch results)
        cursor_ps = conn_ps.cursor()
        cursor_ps.execute(query_ps)
        rows = cursor_ps.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        # Print any errors that occur
        print(f"An error occurred: {e}")
    finally:
        # Ensure connections are closed, if they were opened
        if conn_ps:
            conn_ps.close()
            print("PostgreSQL connection closed")

def create_product_stg_tab(df):
    # Establish connection to PostgreSQL
    conn_ps = config.config_postgre()
    
    



