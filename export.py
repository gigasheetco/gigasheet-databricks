import argparse


def export(dbfs_path: str, catalog: str, schema: str, table: str):
    """
    Save csv from dbfs as table in databricks.

    Args:
        dbfs_path (str): The path of the CSV file in DBFS.
        catalog (str): The name of the catalog where the Delta table will be created.
        schema (str): The name of the schema where the Delta table will be created.
        table (str): The name of the Delta table.

    Raises:
        Exception: If an error occurs during the export process.

    """
    try:
        df = (
            spark.read.option("header", "true")
            .option("escape", '"')
            .option("multiLine", "true")
            .csv(dbfs_path)
        )
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.columnMapping.mode", "name")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{table}")
        )
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        # always try to remove the file from dbfs
        dbutils.fs.rm(dbfs_path, True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some parameters.")

    parser.add_argument("--dbfs-path", type=str, required=True, help="DBFS File Path")
    parser.add_argument("--catalog", type=str, required=True, help="Catalog name")
    parser.add_argument("--schema", type=str, required=True, help="Schema name")
    parser.add_argument("--table", type=str, required=True, help="Table name")

    args = parser.parse_args()

    export(args.dbfs_path, args.catalog, args.schema, args.table)
