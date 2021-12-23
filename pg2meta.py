#!/usr/bin/python3

import sys, getopt, argparse, psycopg2, uuid, random


class Metadata:
    def __init__ (self, schema, tablename, count):
        self.schema = schema
        self.tablename = tablename
        self.count = count

class MetaRecord:
    def __init__ (self, fieldname, fieldtype):
        self.attribuut = fieldname
        self.datatype = fieldtype
        
        self.semantiek = "" 
        self.business_key = ""
        self.waarden = "" 
        self.primaire=""
        self.min="" 
        self.gem=""
        self.max=""
        self.vulling_totaal=""
        self.vulling_perc=""
        self.geometry_type = ""
        self.invalid_geoms = "" 
        self.opm = "" 
        self.vulling_distinct = "" 
        self.vulling_distinct_values = "" 
        self.geom_types = "" 
        self.vulling_sample_values = ""

class pg2meta:
    def __init__(self, host, port, db, user, password, schema, tables):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.schema = schema
        self.tables = tables
        #CSV seperator to be used
        self.seperator = ";"
        self.csvfile = None 
        self.prefix = "" 
         
        self.conn = psycopg2.connect(f"dbname={db} user={user} host={host} password={password}")

        self.dictTypes = {
            "ARRAY": ("array", True, False ),
            "USER-DEFINED": ("user-defined", True, False),
            "bigint": ("bigint", False, True),
            "boolean": ("boolean", False, False),
            "character varying": ("varchar", False, False),
            "date": ("date", False, False),
            "double precision":("double", False, True),
            "integer":("integer", False, True),
            "jsonb": ("jsonb", True, False),
            "name": ("name", True, False),
            "numeric":("numeric", False, True),
            "text": ("text", False, False),
            "timestamp with time zone": ("timestamp with time zone", True, False),
            "timestamp without time zone": ("timestamp without time zone", True, False),
            "uuid": ("varchar", True, False),
        }
        
        #if no tables suplied, try to read tables from schema.
        if self.tables == None or len (self.tables) == 0:
            self.tables = self.getTablesFromSchema()
    
    def getTablesFromSchema (self):
        print (f"No tables given, reading from schema {self.schema}")
        
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{self.schema}' AND table_type='BASE TABLE'"
        
        values=[]
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            values.append(row[0])
            print(row[0])
        cur.close()
        return values
    
    def countRecords(self, tablename):
        print (f"counting records of {self.schema}.{tablename}")
        
        sql = f"select count(*) from {self.schema}.{tablename}"
        
        cur = self.conn.cursor()
        cur.execute(sql)
    
        rec = cur.fetchone()
        count = rec[0]
        cur.close()
        
        return count
    
    def getNullCount (self, tablename, fieldname):

        print ("counting null values of {0}".format(fieldname))
        sql = "SELECT COUNT(*) FROM {1}.{2} where {0} is null".format(fieldname, self.schema, tablename)
        cur = self.conn.cursor()
        cur.execute(sql)
        rec = cur.fetchone()
        count = rec[0]
        cur.close()
        return count


    def getDistinctValueCount (self, tablename, fieldname):

        print (f"counting distinct values of {fieldname}")
        sql = f"SELECT COUNT(*) FROM (SELECT DISTINCT {fieldname} FROM {self.schema}.{tablename}) AS temp"
        cur = self.conn.cursor()
        cur.execute(sql)
        rec = cur.fetchone()
        count = rec[0]
        cur.close()
        return count
        
    def getDistinctValues (self, tablename, fieldname):
    
        #1: check number of distinct values
        print ("getting distinct values {0}".format(fieldname))
        sql = f"SELECT {fieldname}, count(*) FROM {self.schema}.{tablename} group by {0} order by count(*) desc"
        values=[]
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            values.append( (row[0], row[1]))
        cur.close()
        return values
        
    def getSampleValues (self, tablename, fieldname, count_records):
        print ("getting sample values of {0}".format(fieldname))
        
        try: 
            v = 200
            sql = f"SELECT distinct({fieldname})  FROM {self.schema}.{tablename} TABLESAMPLE bernoulli ( ({v}*100.0)/{count_records})"
            values=[]
           
            
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            
            #now return random 5 from rows.
            rows_random = random.choices(rows, k=5)
            for row in rows_random:
                v = row[0]
                if not v in values: 
                    values.append(row[0])
            cur.close()
            return values
        except: 
            self.conn.rollback()
            cur.close()
            
            limit=5
            sql = f"SELECT distinct({fieldname})  FROM {self.schema}.{tablename} limit {limit}"
            values=[]
           
            
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            
            #now return random 5 from rows.
            rows_random = random.choices(rows, k=25)
            for row in rows_random:
                v = row[0]
                if not v in values: 
                    values.append(row[0])
            cur.close()
            return values
        
    def getMinAvgMax (self, tablename, fieldname, isnumeric):
        print (f"getting min, avg, max of {fieldname}")
        
        try:
            if (isnumeric):
                sql = f"SELECT min({fieldname}),avg({fieldname}),max({fieldname})  FROM {self.schema}.{tablename} where not {fieldname} is null"
            else:
                sql = f"SELECT min({fieldname}::numeric),avg({fieldname}::numeric),max({fieldname}::numeric)  FROM {self.schema}.{tablename} where not {fieldname} is null and {fieldname} <> '' "
            cur = self.conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            
            #now return random 5 from rows.
            values = (row[0],row[1], row[2])
            cur.close()
            return values
        except Exception as e:
            print (f"Error calculating min/avg/max for {tablename}.{fieldname}: {e}")
            self.conn.rollback()
            cur.close()
            return (-1,-1,-1)

    def getType(self, column_type):
        return self.dictTypes[column_type]
        
    def getGeomType (self, tablename, fieldname):
        print ("getting geom types from {fieldname}. Might take some time"))
        try:
            sql = f"SELECT ST_GeometryType ({fieldname}), count(*) from {self.schema}.{tablename} where not {fieldname} is null group by ST_GeometryType ({fieldname})"
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()    
            
            values=[]
            for row in rows:
                values.append( (row[0],row[1]) )
            cur.close()
            return values
        except Exception as e:
            print (f"Error getting Geom type from {tablename}.{fieldname}: {e}")
            self.conn.rollback()
            cur.close()
            return ("",0)
            
    def countInvalidGeometries (self, tablename, fieldname):
        print (f"counting invalid geometries types from {fieldname}. Might take some time")
        try:
            sql = "SELECT count(*) from {self.schema}.{tablename} where not ST_IsValid ({fieldname})"
            cur = self.conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()    
            
            value = row[0]
            cur.close()
            return value
        except Exception as e:
            print (f"Error counting invalid geometries from {tablename}.{fieldname}: {e}")
            self.conn.rollback()
            cur.close()
            return ""
     
    def writeMeta(self, tablename, fields):
        file = open(tablename.replace("\"","") +".txt","w", encoding="utf-8")
        count_records = self.countRecords(tablename)
        
        metadata = Metadata (self.schema, tablename, count_records)
        
        file.write(f"Tabelnaam: {metadata.schema}.{metadata.tablename}\n") 
        file.write(f"Aantal records: {metadata.count}\n\n") 
        
        for (fieldname, fieldtype) in fields:
            metarecord = MetaRecord (fieldname, fieldtype)
            
            file.write(f"\nAttribuut: {fieldname}\nType: {fieldtype}\n")
            
            count_nullvalues = self.getNullCount(tablename, fieldname)
            file.write(f"Aantal NULL waarden: {count_nullvalues}\n")
            
            metarecord.vulling_totaal = metadata.count - count_nullvalues
            metarecord.vulling_perc = 100 * ( float(metarecord.vulling_totaal)/float(metadata.count))
            
            count_distinctvalues = self.getDistinctValueCount(tablename, fieldname)
            file.write(f"Aantal verschillende waarden: {count_distinctvalues}\n")
            
            metarecord.vulling_distinct = count_distinctvalues
            
            if (fieldtype.lower() == "user-defined"):
                geom_types = self.getGeomType (tablename, fieldname) #format: array of (type, count)
                metarecord.geom_types = geom_types
                
                count_invalid_geoms = self.countInvalidGeometries (tablename, fieldname)
                metarecord.invalid_geoms = count_invalid_geoms
            elif (count_distinctvalues < 25):
                distinctvalues =self.getDistinctValues(tablename, fieldname)
                file.write("Waarden: \n")
                for (dv, count_dv) in distinctvalues:
                    file.write(f"\t{dv}\t{count_dv}\n")
                    
                metarecord.vulling_distinct_values =  distinctvalues   
            else:
                (stype, blog, bnum) = self.getType(fieldtype)
                print (stype, blog, bnum)
                
                if (not blog) and ("wkt" not in fieldname):
                    minavgmax = self.getMinAvgMax(tablename, fieldname, bnum)
                    (min,avg,max) = minavgmax
                    file.write (f"min: {min}, gem: {avg}, max: {max}\n")
                    
                    metarecord.min = min
                    metarecord.gem = avg
                    metarecord.max = max
                        
                samplevalues = self.getSampleValues (tablename, fieldname, count_records)
                file.write("Voorbeeld waarden: \n")
                for sv in samplevalues:
                    file.write(f"\t{sv}\n")
                    
                metarecord.vulling_sample_values = samplevalues    
                    
            file.flush()    
            
            self.writeMetarecord (metarecord)
            
        file.close()
        
    def writeMetarecord (self, metarecord):   
        min = metarecord.min
        gem = metarecord.gem
        max = metarecord.max
        if (metarecord.min=="" or (metarecord.min==-1 and metarecord.gem==-1 and metarecord.max==-1)):
            min = ""
            gem = ""
            max = "" 

        if (len (metarecord.geom_types) > 0):
            i = 1
            for (geom_type, count) in metarecord.geom_types:
                metarecord.geometry_type =  metarecord.geometry_type + geom_type + "(" + str(count) + ")" 
                if (i<len (metarecord.geom_types)): 
                    metarecord.geometry_type = metarecord.geometry_type + chr(10)
                i+=1
            metarecord.geometry_type = '"' + metarecord.geometry_type  + '"' 

        # domeinwaarde
        domeinwaarde = f"Verschillende waarden: {metarecord.vulling_distinct}"
        
        if len (metarecord.vulling_distinct_values) > 0:
            for (dv, count_dv) in metarecord.vulling_distinct_values:
                domeinwaarde += chr (10)
                domeinwaarde += f"{dv} ({count_dv})"
        
        if len(metarecord.vulling_sample_values) > 0: 
            domeinwaarde += chr(10) + "Voorbeelden: "
            for sample_value in metarecord.vulling_sample_values:
                domeinwaarde += chr (10)
                domeinwaarde += f"{sample_value}"
        
        domeinwaarde = '"' + domeinwaarde + '"'
        
        r = "\n{1}{0}{2}{0}{3}{0}{4}{0}{5}{0}{6}{0}{7}{0}{8}{0}{9}{0}{10}{0}{11}{0}{12}{0}{13}{0}{14}".format(self.seperator, metarecord.attribuut,metarecord.datatype,"","", domeinwaarde,"", min, gem, max, metarecord.vulling_totaal, metarecord.vulling_perc, metarecord.geometry_type, metarecord.invalid_geoms, "")
        
        self.csvfile.write (r)
        
    def writeTable(self, tablename):
        self.csvfile = open(self.prefix + tablename.replace("\"","") +".csv","w")
        
        fields =[]
        
        cur = self.conn.cursor()
        sql = "SELECT column_name, ordinal_position, column_default, is_nullable, data_type FROM INFORMATION_SCHEMA.COLUMNS where table_schema='{}' and table_name='{}'".format(self.schema, tablename.replace("\"",""))

        cur.execute(sql)
    
        recs = cur.fetchall()

        self.writeHeader(self.csvfile)

        for row in recs:
            column_name = row[0]
            column_type = row[4]
            
            (stype, blog, bnum) = self.getType(column_type)
            fields.append ( (column_name, column_type) )

        cur.close()
        
        self.writeMeta(tablename, fields)
        self.csvfile.close()
        
    def writeTablesFromSchema(self):
        cur = self.conn.cursor()
        sql = f"SELECT distinct(table_name) FROM INFORMATION_SCHEMA.COLUMNS where table_schema='{self.schema}' "

        cur.execute(sql)
    
        recs = cur.fetchall()
        for row in recs:
            table_name = row[0]
            self.writeClass (table_name)
            
        cur.close()
   
        
    def build(self, prefix=""):
        self.prefix = prefix  
        if (len(self.tables)==0):
            self.writeTablesFromSchema () 
        else:
            for table in self.tables:
                self.writeTable (table)

        self.conn.close()
       
    def writeHeader(self, file):
        h='''attribuut{0}datatype{0}semantiek{0}Business key{0}(domein)waarde{0}primaire{0}MIN waarde{0}GEM waarde{0}MAX waarde{0}Vullingsgraad (totaal){0}Vullingsgraad(perc){0}Geometrie type{0}Invalid geometrien{0}Opmerkingen/vragen'''.format(self.seperator)
        file.write (h)
        
    def writeFooter(self):
        h=''''''
        #self.file.write (h)

    
    def generateID(self):
        return uuid.uuid4().hex
    

if __name__ == "__main__":
    #TODO use command parameters
    pg2meta = pg2meta ("db host", "db port", "db database", "db user", "db password", "schema", ["array of tables, if empty all schema is analysed"])
    pg2meta.build()