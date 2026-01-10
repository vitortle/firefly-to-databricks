# List all files in the volume
files = dbutils.fs.ls("/Volumes/firefly/raw/landing/")

for file in files:
    # gets the name of the files without the parquet extension
    table_name = file.name.replace(".parquet", "")
    file_path = file.path
    
    print(f"Creating the bronze table: {table_name}")
    
    # reads the parquet files and store them as a Delta table in the raw schema
    df = spark.read.parquet(file_path)
    df.write.format("delta").mode("overwrite").saveAsTable(f"firefly.raw.bz_{table_name}")