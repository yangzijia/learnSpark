import sys
import time
import MySQLdb
from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext, SQLContext

# /usr/bin/spark-submit --jars "/home/engyne/spark/ojdbc7.jar" --master local  /home/engyne/spark/SparkDataBase.py

conf = SparkConf().setAppName('inc_dd_openings')
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

reload(sys)
sys.setdefaultencoding("utf-8")

mysql_url = "192.168.1.111"
mysql_user = "root"
mysql_password = "xxx"
mysql_db = "xxx"

get_df_url = "jdbc:oracle:thin:@//192.168.1.xxx:1521/ORCLPDB"
get_df_driver = "oracle.jdbc.driver.OracleDriver"
get_df_user = "xxx"
get_df_password = "xxx"


# insert		update		delete
def conMysqlDB_exec(sqlStr):
    db = MySQLdb.connect(mysql_url, mysql_user, mysql_password, mysql_db, charset='utf8' )
    cursor = db.cursor()
    try:
        cursor.execute(sqlStr)
        db.commit()
        result = True
    except:
        print("---->MySqlError: execute error")
        result = False
        db.rollback()
    db.close
    return result

# select
def conMysqlDB_fetchall(sqlStr):
	db = MySQLdb.connect(mysql_url, mysql_user, mysql_password, mysql_db, charset='utf8' )
	cursor = db.cursor()
	results = []
	try:
		cursor.execute(sqlStr)
		results = cursor.fetchall()
	except:
		print("---->MySqlError: unable to fecth data")
	db.close
	return results

#  import
def importData(isIncremental, isOverwrite):
    time_start = time.time()
    findJobSql = "SELECT * FROM job where status=1"
    result = conMysqlDB_fetchall(findJobSql)
    resultInfoList = []
    for i, val in enumerate(result):
        databaseName = val[1]
        tableName = val[2]
        partitionColumnName = val[3]
        partitionColumnDesc = val[4]
        checkColumn = val[5]
        lastValue = val[6]
        
        sqlContext.sql("use %s" % databaseName)
        
        if isIncremental:
            df = getDF("(select * from %s where to_char(%s, 'yyyy-MM-dd')>'%s')" % (tableName, checkColumn, lastValue))
            try:
                nowLastValue = df.rdd.reduce(max)[checkColumn]
                o2hBase(df, databaseName, tableName, partitionColumnName, partitionColumnDesc, isIncremental, isOverwrite)
                updataJobSql = "UPDATE job SET last_value='%s' WHERE table_name='%s'" % (nowLastValue, tableName)
                if conMysqlDB_exec(updataJobSql):
                    print("---->SUCCESS: incremental import success")
                    resultInfoList.append("SUCCESS: %s import success" % tableName)
            except ValueError:
                print("---->INFO: No new data added!")
                resultInfoList.append("INFO: %s   ValueError(No new data added!)" % tableName)
                pass
            except:
                print("---->ERROR: other error")
                resultInfoList.append("ERROR: %s has other error" % tableName)
                pass
               
        else:
            df = getDF(tableName)
            try:
                o2hBase(df, databaseName, tableName, partitionColumnName, partitionColumnDesc, isIncremental, isOverwrite)
                print("---->INFO: import success")
                resultInfoList.append("SUCCESS: %s import success" % tableName)
            except:
                print("---->ERROR: import error")
                resultInfoList.append("ERROR: %s import error" % tableName)
                pass
    print("RESULT:")
    for i, val  in enumerate(resultInfoList):
        print(val)
    time_end = time.time()
    print("---->INFO: time cost", (time_end - time_start)/60, "m")
        
def max(a, b):
    if a>b:
        return a
    else:
        return b


        
def getDF(tableName):
    try:
        df = sqlContext.read.format("jdbc") \
            .option("url", get_df_url) \
            .option("driver", get_df_driver) \
            .option("dbtable", tableName) \
            .option("user",  get_df_user).option("password", get_df_password) \
            .load()
    except:
        print("---->DF_ERROR: get df error")
    return df

    

# o2h
def o2hBase(df, databaseName, tableName, partitionColumnName, partitionColumnDesc, isIncremental, isOverwrite):
    sqlContext.sql("use %s" % databaseName)
    schema = df.schema
    cnSql, columnNameSql = getTableDesc(df, partitionColumnName)
    
    # rdd = df.map(lambda x : [x[processLineList[i]].replace("\n","").replace("\r","") for i in range(len(processLineList))])
    rdd = df.map(lambda x : [(x[i].replace("\n","").replace("\r","") if isinstance(x[i], unicode) or isinstance(x[i], str) else x[i]) for i in range(len(x))])
    df = sqlContext.createDataFrame(rdd, schema)
    
    df.registerTempTable("temp%s" % tableName)
    
    if partitionColumnName == "":
        if isIncremental:
                saveSql = "insert into table %s select * from temp%s" % (tableName, tableName)
        else:
            if isOverwrite:
                saveSql = "insert overwrite table %s select * from temp%s" % (tableName, tableName)
            else:
                saveSql = "create table %s as select * from temp%s" % (tableName, tableName)
    else:
        sqlContext.sql("set hive.exec.dynamic.partition=true")
        sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        sqlContext.sql("SET hive.exec.max.dynamic.partitions=100000")
        sqlContext.sql("SET hive.exec.max.dynamic.partitions.pernode=100000")
        if isIncremental:
            saveSql = "insert into table %s partition(%s) SELECT %s,%s FROM temp%s" % (tableName, partitionColumnName, cnSql, partitionColumnName, tableName)
        else:
            if !isOverwrite:
                # dynamic partition create table
                createTableSql = "create table %s (%s)PARTITIONED BY (%s %s) row format delimited fields terminated by '\t'  LINES TERMINATED BY '\n'" % (tableName, columnNameSql, partitionColumnName, partitionColumnDesc)
                sqlContext.sql(createTableSql)
                print("---->INFO: dynamic partition create success")
            saveSql = "insert overwrite table %s partition(%s) SELECT %s,%s FROM temp%s" % (tableName, partitionColumnName, cnSql, partitionColumnName, tableName)
    sqlContext.sql(saveSql)
    sqlContext.dropTempTable("temp%s" % tableName)


def getTableDesc(sqlDF, partitionColumnName):
    columnNameSql = ""
    cnSql = ""
    dfTypeList = sqlDF.dtypes
    for i, val in enumerate(dfTypeList):
        if (val[0] != partitionColumnName):
            columnNameSql = columnNameSql + val[0] + " " + val[1]
            cnSql = cnSql + val[0]

            if i + 1 != len(dfTypeList):
                columnNameSql = columnNameSql + ","
                cnSql = cnSql + ","
    return cnSql, columnNameSql

    
if __name__ == '__main__':
    #Incremental import
    importData(True,True)
    
    #General import
    # importData(False, True)

