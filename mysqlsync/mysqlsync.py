import sys
class mysqlSync:
    mdb=""   
    cur=""
    tables={}   
    jtables={}
    ptype=""
    config={}
    items={}
    errors=[]   
    delprompt=True
    trows=0
    isconnected=True
    autodel=False
    v="1.0.0"
    def __init__(self,dbc):        
        if self.cfg(dbc):
            self.connect()
            if self.isconnected:
                self.gettables()
                if self.ptype in ["status","update","delete","sync"]:
                    self.getjson()
                    self.compare()
                self.process()
    def cfg(self,dbc):
        l=len(sys.argv)
        if l<2:
            self.errors.append("Min 2 arguements required")
            return 
        self.ptype=sys.argv[1]
        c=sys.argv[2] if l>=3 else "default"        
        if c not in dbc:
            self.errors.append("invalid config")
            return False
        self.config=dbc[c]
        self.delprompt=True if ("delprompt" in dbc[c] and dbc[c]['delprompt']==True) or "delprompt" not in dbc[c]  else False
        self.autodel=True if "autodelete" in dbc[c] and dbc[c]['autodelete'] else False
        return True
    def connect(self):       
        import mysql.connector
        try:
            self.mdb = mysql.connector.connect(
                host=self.config['host'],
                user=self.config['user'],
                passwd=self.config['pass'],
                database=self.config['db']
            )       
            self.cur=self.mdb.cursor()  
            self.isconnected=True
        except Exception as e:
            self.errors.append("Mysql error:"+str(e))
            self.isconnected=False            
    def gettables(self):
        if self.cur=="":
            return False
        self.cur.execute("SHOW TABLES")
        tbl=self.cur.fetchall()
        for x in tbl:
            t=x[0]            
            self.cur.execute("DESCRIBE `"+t+"`")
            items=self.cur.fetchall()
            fn = [i[0] for i in self.cur.description]
            tbls=[]
            for r in items:
                i=0
                tb={}
                for f in fn:                    
                    tb[f]=r[i]
                    i=i+1
                tbls.append(tb)    
            self.tables[t]=tbls           
    def getjson(self,f="mysqlschema.json"):
        import json  
        if "schema" in self.config:
            f=self.config['schema']
        try:    
            with open(f,'r') as fl:  
                self.jtables = json.load(fl)
        except Exception as e:
            print("File Error "+str(e))          
    def compare(self):
        jkeys=list(self.jtables.keys())
        mkeys=list(self.tables.keys())
        ntable=[]
        deltable=[]
        nclms={}
        altrclms={}
        delcol={}
        for k in jkeys:
            if k not in mkeys:
                ntable.append(k)
            else:
                mt=self.tables.get(k)    
                mt=self.psort(mt,"Field") 
                jt=self.psort(self.jtables.get(k),"Field")  
                for j in jt:   
                    if j not in mt:
                        if not k in nclms:
                            nclms[k]={}
                        nclms[k][j]=jt[j]
                    else:
                        mta=mt[j]
                        jta=jt[j]                       
                        for f,t in jta.items():   
                            if f in mta and t!=mta[f]:                                 
                                if not k in altrclms:
                                    altrclms[k]={}
                                if not j in altrclms[k]:
                                    altrclms[k][j]={}
                                altrclms[k][j][f]=t     
                for j in mt:
                    if not j in jt:
                        if not k in delcol:
                            delcol[k]=[]
                        delcol[k].append(j)
        for k in mkeys:
            if k not in jkeys:
                deltable.append(k)       
        self.items={"newtbl":ntable,"newcol":nclms,'altcol':altrclms,'deltbl':deltable,'delcol':delcol}          
    def process(self):
        fn=self.ptype
        try:            
            f=getattr(self,fn)
            f()
        except Exception as e:           
            print("invalid query "+fn+" "+str(e))   
            self.errors.append("invalid attribute "+fn)
    def createjson(self,f="mysqlschema.json"):
        import json       
        if "schema" in self.config:
            f=self.config['schema']
        with open(f, 'w') as fp:
            json.dump(self.tables,fp,sort_keys=False, indent=4)
        print("JSON file created at `{}`".format(f))
    def sync(self):
        self.update()
        if self.autodel:    
            self.delete(False) 
        elif len(self.items['deltbl'])>0 or len(self.items['delcol'])>0:
            print("Some items to be deleted use `delete` operation")            
    def update(self):
        self.addtable()
        self.addcol()
        self.chcol()            
        if len(self.items['newtbl'])==0 and len(self.items['newcol'])==0 and len(self.items['altcol'])==0:
            print("Nothing to Update")           
    def delete(self,msg=True): 
        self.deltable()
        self.dropcol()
        if len(self.items['deltbl'])==0 and len(self.items['delcol'])==0 and msg:
            print("Nothing to Delete")   
    def psort(self,d,t='Field'):
        s={}
        for i in d:
            s[i[t]]=i
        return s    
    def gettype(self,t,col):
        op="";
        if t in self.jtables:
            ps=self.psort(self.jtables[t])
            op=ps[col]["Type"] if col in ps and "Type" in ps[col] else ""
        return op
    def addtable(self):        
        for t in self.items['newtbl']:
            cols=self.jtables[t]
            keys=""
            qry="CREATE TABLE IF NOT EXISTS `"+t+"` ("
            for c in cols:    
                if 'Key' in c  and c['Key']!="":
                    if c['Key']=="PRI":
                        keys+=" PRIMARY KEY  (`"+c['Field']+"`) "
                    elif c['Key']=="UNI":
                        if keys!="":
                            keys+=", "
                        keys+=" UNIQUE (`"+c['Field']+"`) "
                    elif c['Key']=="MUL":    
                        if keys!="":
                            keys+=", "
                        keys+=" INDEX (`"+c['Field']+"`) "  
                null=" NOT NULL "
                if 'Null' in c:
                    if c['Null'].lower()=="yes":
                        null=" NULL "
                    elif c['Null'].lower()=="no":
                        null=" NOT NULL "                    
                default=""               
                if 'Default' in c  and c['Default']!=None:                   
                    if c['Default']=='CURRENT_TIMESTAMP':
                        default=" default "+c['Default']+" "
                    else:
                        default=" default '"+c['Default']+"' "
                elif 'Default' in c  and c['Default']==None and 'Null' in c and c['Null'].lower()=="yes":
                    default=" default NULL "                            
                extra=""
                if 'Extra' in c:
                    extra=c['Extra']
                qry+="`"+c['Field']+"` "+c['Type']+null+default+extra+", "            
            qry+=keys+" );"	
            #print(qry)   
            self.cur.execute(qry)
            self.trows+=self.cur.rowcount
            print("Table "+t+" created")
    def colqry(self,ctype="ADD"):
        qtps={"ADD":"newcol","CHANGE":"altcol"}
        tp=qtps[ctype] if ctype in qtps else 'newcol'        
        for tab,cols in self.items[tp].items():            
            sql=" ALTER TABLE `"+tab+"`"
            keys=""
            i=0;
            changed=[]            
            for ck,cv in cols.items():                  
                if 'Key' in cv  and cv['Key']!="":
                    if cv['Key']=="PRI":
                        keys+=", ADD PRIMARY KEY  (`"+ck+"`) "
                    elif cv['Key']=="UNI":
                        if keys!="":
                            keys+=", "                        
                        keys+=", ADD UNIQUE (`"+ck+"`) "
                    elif cv['Key']=="MUL":
                        if keys!="":
                            keys+=", "                        
                        keys+=", ADD INDEX (`"+ck+"`) "                            
                null=" NOT NULL "
                if 'Null' in cv:
                    if cv['Null'].lower()=="yes":
                        null=" NULL "
                    elif cv['Null'].lower()=="no":
                        null=" NOT NULL "  
                default="";
                if 'Default' in cv  and cv['Default']!=None:   
                    if cv['Default']=='CURRENT_TIMESTAMP':
                        default=" default "+cv['Default']+" "
                    else:
                        default=" default '"+cv['Default']+"' "
                extra=""
                if 'Extra' in cv:
                    extra=cv['Extra']      
                if ctype=="CHANGE":    
                    rename=ck
                    changed.append(ck)
                    ct=cv['Type'] if "Type" in cv else self.gettype(tab,ck)
                    if 'Rename' in cv:
                        rename=cv['Rename']
                    sql+=" CHANGE `"+ck+"` `"+rename+"` "+ct+null+default+extra+" ";                    
                else:    
                    changed.append(ck)
                    sql+=" ADD `"+cv['Field']+"` "+cv['Type']+null+default+extra+" "
                i+=1 
                sql+=keys
                if len(cols)>i:
                    sql+=", "
            sql+=";"  
            if ctype=="CHANGE":    
                print("Changed {} in Table `{}`".format(",".join(changed),tab))
            else:             
                print("Added {} in Table `{}`".format(",".join(changed),tab))
            self.cur.execute(sql)
            self.trows+=self.cur.rowcount
            #print(sql)    
    def addcol(self):
        self.colqry("ADD")
    def chcol(self):
        self.colqry("CHANGE")
    def deltable(self):       
        if self.promptdel("Do You Want to Delete These tables?","deltbl"):
            for t in self.items['deltbl']:
                sql="DROP TABLE `"+t+"`;"
                self.cur.execute(sql)
                self.trows+=self.cur.rowcount
                print("Table {} deleted".format(t))
                #print(sql)  
    def promptdel(self,msg,item):
        isDel=True
        if self.delprompt and len(self.items[item])>0:           
            self.stprint(item)
            ip=input(msg)
            if ip!="y" and ip!="yes":
              isDel=False   
        return isDel      
    def dropcol(self):
        if self.promptdel("Do You Want to Delete These Columns?","delcol"):
            if 'delcol' in self.items and len(self.items['delcol'])>0:
                for dt,dc in self.items['delcol'].items():
                    sql="ALTER TABLE `"+dt+"`" 
                    i=0
                    for cl in dc:
                        sql+=" DROP `"+cl+"` "
                        i+=1
                        if len(dc)>i:
                            sql+=", "
                    sql+=";"                     
                    self.cur.execute(sql)
                    self.trows+=self.cur.rowcount
                    print("Columns {} from Table`{}` deleted".format(",".join(dc),dt))
                    #print(sql)
    def status(self):
        if 'newtbl' in self.items and len(self.items['newtbl'])>0:
            print("New Tables :")
            for t in self.items['newtbl']:
                print("\t"+t)
        if 'newcol' in self.items and len(self.items['newcol'])>0:    
            print("New Columns :")
            for t,v in self.items['newcol'].items():
                print("\t Table:"+t)               
                for c,v1 in v.items():
                    print("\t\t"+c)            
        if 'altcol' in self.items and len(self.items['altcol'])>0:     
            print("Column Changes :")
            for t,v in self.items['altcol'].items():
                print("\t Table:"+t)               
                for c,v1 in v.items():
                    print("\t\t"+c)    
        self.stprint('deltbl')
        self.stprint('delcol')
        if (len(self.items['deltbl'])>0 or len(self.items['delcol'])>0) and (not self.delprompt ):
            print("NB:There will be no confirmation for delete process")
        self.nmsg()
    def nmsg(self):
        ti=len(self.items)
        il=0;
        for k,v in self.items.items():
            if len(v)==0:
                il+=1
        if ti==il:         
            print("Nothing to update") 
        
    def stprint(self,type):
        if type=="deltbl":
            if 'deltbl' in self.items and len(self.items['deltbl'])>0:     
                print("Tables To be Deleted :")
                for dt in self.items['deltbl']:
                    print("\t"+dt)
        if type=="delcol":
            if 'delcol' in self.items and len(self.items['delcol'])>0:     
                print("Columns To be Deleted :")
                for dt,dc in self.items['delcol'].items():
                    print("\t Table:"+dt)     
                    for cl in dc:
                        print("\t\t"+cl) 
    def version(self):
        print(self.v)
    def __del__(self):
        if len(self.errors)>0:
            for e in self.errors:
                print(e)
        if self.cur!="":
            self.cur.close()    
            """ALTER TABLE `child` ADD CONSTRAINT `name` FOREIGN KEY (`index`) REFERENCES `parent`(`paer id`) ON DELETE RESTRICT ON UPDATE RESTRICT;"""
def init(dbconfig):
    return mysqlSync(dbconfig)