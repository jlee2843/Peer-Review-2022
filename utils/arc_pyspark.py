from pathlib import Path
from typing import Optional

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql._typing import UserDefinedFunctionLike
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from typing.io import IO


# if __all__ is not set then all attributes in the module can be accessed by import expect for those attributes that
# start with '_'. When __all__ is defined only those attributes are accessible.
# __all__ = ['get_slurm_id', 'get_slurm_dir']


def get_slurm_id() -> str:
    import subprocess
    slurmID: Optional[IO[bytes]]

    try:
        slurmID = subprocess.Popen("squeue -u $USER | tail -1 | awk {'print $1'}", shell=True,
                                   stdout=subprocess.PIPE).stdout
    except BaseException:
        slurmID = subprocess.Popen("ssh -f arc squeue -u $USER | tail -1 | awk {'print $1'}", shell=True,
                                   stdout=subprocess.PIPE).stdout
    return str(slurmID.read().rstrip(), 'utf-8')


def get_slurm_dir() -> Path:
    return Path('/scratch', get_slurm_id())


# returns (SparkContext, SqlContext) objects.
def get_spark_sql_context(pyfiles: str) -> tuple[SparkContext, SparkSession]:
    import os

    configLines = [tuple(a.rstrip().split(" ")) for a in open(os.environ['SPARK_CONFIG_FILE']).readlines()]
    conf = SparkConf()
    conf.setAll(configLines)
    conf.setMaster("spark://%s:%s" % (os.environ['SPARK_MASTER_HOST'], os.environ['SPARK_MASTER_PORT']))
    conf.set("spark.speculation", "false")
    conf.set("spark.files", pyfiles)
    conf.set("spark.submit.pyFiles", pyfiles)
    print(f'configs:\n{conf.getAll()}')

    # sc.getConf() # get spark session and configuration
    sqlCtx = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = sqlCtx.sparkContext
    sc.addPyFile(pyfiles)
    print(f'spark ui: {sc.uiWebUrl}')
    # SQLContext(sc, sparkSession=sc.getConf())
    # sqlCtx=pyspark.sql.SparkSession.builder.getOrCreate()
    temp = sc, sqlCtx
    print(temp)
    return sc, sqlCtx


# Create custom function
def title_case(x: str) -> str:
    return x.strip().title()


def upper_case(x: str) -> str:
    return x.strip().upper()


# PEP 484
upperCaseUDF: UserDefinedFunctionLike = udf(lambda x: upper_case(x), StringType())
titleCaseUDF: UserDefinedFunctionLike = udf(lambda x: title_case(x), StringType())
