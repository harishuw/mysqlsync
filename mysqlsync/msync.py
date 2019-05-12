from mysqlsync import mysqlsync
dbconfig={
    "default":{
        "host":"localhost",
        "user":"",
        "pass":"",
        "db":"msync_test",
        "schema":"schema.json",
        "delprompt":True
    },
    "other":{
        "host":"localhost",
        "user":"",
        "pass":"",
        "db":"msync_test",
        "autodelete":True,
        "delprompt":False,
        "schema":"schema2.json",
    }
}     
       
mysqlsync.init(dbconfig)