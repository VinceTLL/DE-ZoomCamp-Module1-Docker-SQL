
import duckdb
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import pandas as pd
from pathlib import Path
import sys



dtype = {
  'LocationID': 'int64',
 'Borough':"string" ,
 'Zone': "string",
 'service_zone':"string" }


def read_csv_file(file_name: str, batch_size:int,file_format:str):
  
  """ """

  try:
      url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/{file_name}.{file_format}"
      iter_data = pd.read_csv(url,dtype=dtype,chunksize=batch_size,iterator=True)
      return iter_data
  except Exception as e:
     print(f"Unbale to read csv file {e}")
     return
  

def get_duckdb_reader(file_name: str, batch_size: int, year: int, month: int, file_format:str):

    """
    Split parquet data into Batches using duckDB
    
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
    url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}_{year}-{month:02d}.{file_format}"
    dr_path = Path(Path.cwd(),"query.sql")

    if dr_path.exists:

       query = dr_path.read_text().format(url)
    
    else:
        query = f"SELECT * FROM '{url}'"

    try:
        record_batch_reader = con.execute(query).fetch_record_batch(rows_per_batch=batch_size)
        con.close()
        
        return record_batch_reader
    
    except Exception as e:
        print(f"Error initializing DuckDB streamL: {e}")
        sys.exit(1)




@click.command()
@click.option('--user',default= 'root',help = 'PostgreSQL user')
@click.option('--password',default= 'root',help = 'PostgreSQL password')
@click.option('--host',default= 'localhost',help = 'PostgreSQL localhost')
@click.option('--port',default= 5432,type=int,help = 'PostgreSQL port')
@click.option('--database',default= 'ny_taxi',help = 'PostgreSQL database')
@click.option('--target_table',default= 'yellow_taxi_data',help = 'PostgreSQL table')
@click.option('--file_name',default= 'green_tripdata',help = 'name of file')
@click.option('--batch_size',default= 10000,type=int,help = 'size of each data batch')
@click.option('--year',default= 2025,type=int,help = 'Year of data')
@click.option('--month',default= 11,type=int,help = 'Month of data')
@click.option('--file_format',default= 'parquet',help = 'format of source file')

def run_ingestion(user,password,host,port,database,target_table,file_name,batch_size,year,month, file_format):

    """
    Ingest data into PostgreSQL database into chunks
    
    :param user: DataBase user name
    :param password: DataBase user password
    :param host: Host name
    :param port: Port
    :param database: 
    :param target_table: table to load the data
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
        engine.dispose()
        return 
    


    # Ingest data to Database
    if file_format == "csv":
        batch_reader = read_csv_file(file_name,batch_size,file_format)
    else:
        batch_reader = get_duckdb_reader(file_name,batch_size,year,month,file_format)


    if batch_reader is None:
        print("No data found or error reading stream.")
        return
    
    first = True
    try:
        for batch in tqdm(batch_reader):

            try:
                chunk_df = batch.to_pandas()
            except AttributeError:
                chunk_df = pd.DataFrame(batch)


            if first:
                chunk_df.to_sql(name = target_table, con = engine, if_exists = "replace")
                first = False
            else:
                chunk_df.to_sql(name = target_table, con =engine, if_exists = 'append')
    
    except Exception as e:
        print(f"Error during ingestion {e}")
    
    finally:
        engine.dispose()
        print("Ingestion job finished.")


#docker run -it --rm -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 --network=pg-network --name pgdatabase postgres:13

#docker run -it --rm -e PGADMIN_DEFAULT_EMAIL=admin@admin.com -e PGADMIN_DEFAULT_PASSWORD="root" -v pgadmin_data:/var/lib/pgadmin -p 8085:80 --network=pg-network --name pgadmin dpage/pgadmin4


#docker run -it --network=pg-network taxi_ingest:v001 --user=root --password=root --host=pgdatabase --port=5432 --database=ny_taxi --target_table=yellow_taxi_data --file_name=green_tripdata --batch_size=10000 --year=2025 --month=11 --file_format=parquet
#
