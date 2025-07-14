from datetime import date, datetime
import logging 
import json 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_users_mysql(cursor):
    # Query to get all data from the users table
    cursor.execute("SELECT * FROM users")

    # Fetch all rows
    users = cursor.fetchall()

    # Format results for date/datetime objects
    result = []
    for user in users:
        user_dict = {}
        for key, value in user.items():
            # Handle date/datetime objects
            if isinstance(value, (date, datetime)):
                user_dict[key] = value.isoformat()
            else:
                user_dict[key] = value
        result.append(user_dict)

    return {
        'statusCode': 200,
        'body': result
    }

def get_tables_mysql(cursor):
    # Query to get all tables in the database
    cursor.execute("SHOW TABLES")

    # Fetch all rows
    tables = cursor.fetchall()

    # Extract table names from the result
    table_names = [list(table.values())[0] for table in tables]

    return {
        'statusCode': 200,
        'body': {
            'tables': table_names
        }
    }

def describe_tables_mysql(cursor, table_names):
    result = {}

    for table_name in table_names:
        # Execute DESCRIBE query for each table
        cursor.execute(f"DESCRIBE {table_name}")

        # Fetch all rows
        columns = cursor.fetchall()

        # Format the schema information
        table_schema = []
        for column in columns:
            # Convert any datetime objects to strings
            processed_column = {}
            for key, value in column.items():
                if isinstance(value, (date, datetime)):
                    processed_column[key] = value.isoformat()
                else:
                    processed_column[key] = value
            table_schema.append(processed_column)

        # Get sample values for each column
        try:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_data = cursor.fetchall()

            # Add sample values to the schema
            if sample_data:
                for i, col in enumerate(table_schema):
                    field_name = col['Field']
                    sample_values = [row[field_name] for row in sample_data if field_name in row]
                    # Convert datetime objects in sample values
                    processed_samples = []
                    for val in sample_values:
                        if isinstance(val, (date, datetime)):
                            processed_samples.append(val.isoformat())
                        else:
                            processed_samples.append(val)
                    col['sample_values'] = processed_samples
        except Exception as e:
            # If we can't get sample values, continue without them
            pass

        result[table_name] = table_schema

    return {
        'statusCode': 200,
        'body': {
            'schemas': result
        }
    }

def get_users_postgresql(cursor):
    # Query to get all data from the users table
    cursor.execute("SELECT * FROM users")

    # Fetch all rows
    users = cursor.fetchall()

    # Get column names
    column_names = [desc[0] for desc in cursor.description]

    # Format results as a list of dictionaries
    result = []
    for user in users:
        user_dict = {}
        for i, col_name in enumerate(column_names):
            # Handle date/datetime objects
            if isinstance(user[i], (date, datetime)):
                user_dict[col_name] = user[i].isoformat()
            else:
                user_dict[col_name] = user[i]
        result.append(user_dict)

    return {
        'statusCode': 200,
        'body': result
    }

def get_tables_postgresql(cursor, username):
    # Query to get all tables owned by the specified user
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
    """)

    # Fetch all rows
    tables = cursor.fetchall()

    # Extract table names from the result
    table_names = [table['table_name'] for table in tables]

    return {
        'statusCode': 200,
        'body': {
            'tables': table_names
        }
    }

def describe_tables_postgresql(cursor, table_names):
    result = {}

    for table_name in table_names:
        # Query to get column information for the table
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        # Fetch all rows
        columns = cursor.fetchall()

        # Format the schema information
        table_schema = []
        for column in columns:
            # Convert any datetime objects to strings
            processed_column = {}
            for key, value in dict(column).items():
                if isinstance(value, (date, datetime)):
                    processed_column[key] = value.isoformat()
                else:
                    processed_column[key] = value
            table_schema.append(processed_column)

        # Get sample values for each column
        try:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Add sample values to the schema
            if sample_data:
                for i, col in enumerate(table_schema):
                    col_name = col['column_name']
                    col_index = column_names.index(col_name) if col_name in column_names else -1
                    if col_index >= 0:
                        sample_values = [row[col_index] for row in sample_data]
                        # Convert datetime objects in sample values
                        processed_samples = []
                        for val in sample_values:
                            if isinstance(val, (date, datetime)):
                                processed_samples.append(val.isoformat())
                            else:
                                processed_samples.append(val)
                        col['sample_values'] = processed_samples
        except Exception as e:
            # If we can't get sample values, continue without them
            pass

        result[table_name] = table_schema

    return {
        'statusCode': 200,
        'body': {
            'schemas': result
        }
    }



def test_db(event):
    db_type = event.get('db_type')
    raw_host = event.get('host')
    username = event.get('username')
    password = event.get('password')
    dbname = event.get('dbname')
    event_port = event.get('port')  # Get port from event if available
    #print("port is ",event_port)

    if not all([db_type, raw_host, username, password, dbname]):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing required parameters'})
        }

    try:
        host = raw_host
        endpoint_port = None

        # Check if db_endpoint contains port
        if ':' in raw_host:
            parts = raw_host.split(':')
            host = parts[0]
            #print("host is ", host)
            try:
                endpoint_port = int(parts[1])
                #print("endpoint_port is ", endpoint_port)
            except ValueError:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Invalid port format in db_endpoint'})
                }

        # Choose port based on priority: event > endpoint > default
        if event_port:
            try:
                port = int(event_port)
                print("port in priority is ", port)
            except ValueError:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Invalid port format in event'})
                }
        elif endpoint_port:
            port = endpoint_port
            #print("port in db_endpoint is ", port)
        else:
            port = 3306 if db_type == 'mysql' else 5432
            #print("port in default is ", port)

        # Database connection logic
        if db_type == 'mysql':
            import mysql.connector
            conn = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                port=port,
                database=dbname
            )
        elif db_type == 'postgresql':
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                user=username,
                password=password,
                port=port,
                dbname=dbname
            )
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f"Unsupported db_type: {db_type}"})
            }

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Connection successful'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    finally:
        if 'conn' in locals():
            conn.close()

