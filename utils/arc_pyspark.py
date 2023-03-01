from pathlib import Path
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
 
#__all__ = ['setBasePathName', 'getBasePathName', 'setPath', 'getPath', 'setFileName', 'getFileName', 'get_json_data', 
#           'checkDOI', 'getSparkSqlContext', 'getSlurmID', 'upperCaseUDF', 'titleCaseUDF', 
#           'getSlurmDir']

def getSlurmID() -> str:
    import subprocess
    id = None
    try:
        id = subprocess.Popen("squeue -u $USER | tail -1 | awk {'print $1'}", shell=True, stdout=subprocess.PIPE).stdout
    except Exception:
        id = subprocess.Popen("ssh -f arc squeue -u $USER | tail -1 | awk {'print $1'}", shell=True, stdout=subprocess.PIPE).stdout
    return str(id.read().rstrip(), 'utf-8')
    
def getSlurmDir() -> Path:
    return Path('/scratch', getSlurmID())

# returns (SparkContext, SqlContext) objects.
def getSparkSqlContext(pyfiles: str):
    import os
    import atexit
    import sys
    import re

    import pyspark
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    from pyspark.sql import SparkSession

    conflines=[tuple(a.rstrip().split(" ")) for a in open(os.environ['SPARK_CONFIG_FILE']).readlines()]
    conf=SparkConf()
    conf.setAll(conflines)
    conf.setMaster("spark://%s:%s"% (os.environ['SPARK_MASTER_HOST'],os.environ['SPARK_MASTER_PORT']))
    conf.set("spark.speculation", "false")
    conf.set("spark.files", pyfiles)
    conf.set("spark.submit.pyFiles", pyfiles)
    print(f'configs:\n{conf.getAll()}')
    
    # sc.getConf() # get spark session and configuration
    sqlCtx=SparkSession.builder.config(conf=conf).getOrCreate()
    sc = sqlCtx.sparkContext
    sc.addPyFile(pyfiles)
    print(f'spark ui: {sc.uiWebUrl}')
    #SQLContext(sc, sparkSession=sc.getConf())
    #sqlCtx=pyspark.sql.SparkSession.builder.getOrCreate()
    
    return sc, sqlCtx

# Create custom function
def titleCase(x: str) -> str:
    return x.strip().title()

def upperCase(x: str) -> str:
    return x.strip().upper()

upperCaseUDF = udf(lambda x:upperCase(x), StringType()) 
titleCaseUDF = udf(lambda x:titleCase(x), StringType())

