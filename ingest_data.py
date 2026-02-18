
import duckdb
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
from pathlib import Path
import sys




def get_duckdb_reader(prefix: str, file_name: str, batch_size: int, year: int, month: int):

    """
    Split parquet data into Batches using duckDB
    
    :param prefix: location of data
    :type prefix: str
    :param file_name: name of the file
    :type file_name: str
    :param batch_size: Size of the batch
    :type batch_size: int
    :param year: Year data was collected
    :type year: int
    :param month: Month data was collected
    :type month: int
    
    
    """


    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    url =f"{prefix}{file_name}_{year}-{month:02d}.parquet"
    dr_path = Path(Path.cwd(),"query.sql")

    if Path.exists(dr_path):

        with open("query.sql","r") as q:
            query = q.read().format(url)
    
    else:
        query = f"SELECT * FROM '{url}'"

    try:
        record_batch_reader = con.execute(query).fetch_record_batch(rows_per_batch=batch_size)
        
        return record_batch_reader
    
    except Exception as e:
        print(f"Error initializing DuckDB streamL: {e}")
        sys.exit(1)




@click.command()
@click.option('--year',default= 2025,type=int,help = 'Year of data')
@click.option('--month',default= 11,type=int,help = 'Month of data')
@click.option('--port',default= 5432,type=int,help = 'PostgreSQL port')
@click.option('--user',default= 'root',help = 'PostgreSQL user')
@click.option('--database',default= 'ny_taxi',help = 'PostgreSQL database')
@click.option('--host',default= 'localhost',help = 'PostgreSQL localhost')
@click.option('--password',default= 'root',help = 'PostgreSQL password')
@click.option('--batch_size',default= 10000,type=int,help = 'size of each data batch')
@click.option('--prefix',default= 'https://d37ci6vzurychx.cloudfront.net/trip-data/',help = 'data prefix')
@click.option('--file_name',default= 'green_tripdata',help = 'name of file')
@click.option('--target_table',default= 'yellow_taxi_data',help = 'PostgreSQL table')
def run_ingestion(user,password,host,port,database,target_table,prefix,file_name,batch_size,year,month):

    """
    Ingest data into PostgreSQL database into chunks
    
    :param user: DataBase user name
    :param password: DataBase user password
    :param host: Host name
    :param port: Port
    :param database: 
    :param target_table: table to load the data
    :param prefix: location of data
    :param file_name: name of the file
    :param batch_size: size of the batch
    :param year: year data was collected
    :param month: month data was collected
    
    """

    #Create DB engine
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'

    try:
        engine = create_engine(db_url)

        with engine.connect() as conn:
            pass

    except Exception as e:
        print(f"failed to connect to PostgreSQL: {e}")
        return 
    

    # Ingest data to Database
    batch_reader = get_duckdb_reader(prefix,file_name,batch_size,year,month)

    if batch_reader is None:
        print("No data found or error reading stream.")
        return
    
    first = True
    try:
        for batch in tqdm(batch_reader):
            chunk_df = batch.to_pandas()

            if first:
                chunk_df.to_sql(name = target_table, con = engine, if_exists = "replace")
                first = False
            else:
                chunk_df.to_sql(name = target_table, con =engine, if_exists = 'append')
    
    except Exception as e:
        print(f"Error during ingestion {e}")
    
    finally:
        print("Ingestion job finished.")


