import sqlite3
from datetime import datetime


class KattisDbConn:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.create_tables()

    def create_tables(self):
        self.create_uni_table("global_uni")
        self.create_user_table("global_user")
        self.create_country_table("global_country")

        self.create_uni_table("swe_uni")
        self.create_user_table("swe_user")
        self.create_subdiv_table("swe_subdiv")

        self.create_user_table("chalmers_user")
        

    def create_uni_table(self, table_name):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS {} ('
            'timestamp      INTEGER,'
            'rank           INTEGER,'
            'uni            TEXT,'
            'subdiv         TEXT,'
            'users          INTEGER,'
            'score          REAL'
            ')'.format(table_name)
        )

    def create_user_table(self, table_name):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS {} ('
            'timestamp      INTEGER,'
            'rank           INTEGER,'
            'name           TEXT,'
            'place          TEXT,' # country or subdiv or both
            'uni            TEXT,'
            'score          REAL'
            ')'.format(table_name)
        )

    def create_subdiv_table(self, table_name):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS {} ('
            'timestamp      INTEGER,'
            'rank           INTEGER,'
            'subdiv         TEXT,'
            'score          REAL'
            ')'.format(table_name)
        )

    def create_country_table(self, table_name):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS {} ('
            'timestamp      INTEGER,'
            'rank           INTEGER,'
            'country        TEXT,'
            'users          INTEGER,'
            'unis           INTEGER,'
            'score          REAL'
            ')'.format(table_name)
        )


    def add_uni_rows(self, rows, table_name, timestamp):
        a = []
        for r in rows:
            a.append((
                timestamp,  #timestamp
                int(r[0]),              #rank
                r[1],                   #uni
                r[2],                   #subdiv
                int(r[3]),              #users
                float(r[4])             #score
            ))
        self.conn.executemany("INSERT INTO {} VALUES (?,?,?,?,?,?)".format(table_name), a)

    def add_user_rows(self, rows, table_name, timestamp):
        a = []
        for r in rows:
            a.append((
                timestamp,  #timestamp
                int(r[0]),              #rank
                r[1],                   #name
                r[2],                   #place
                r[3],                   #uni
                float(r[4])             #score
            ))
        self.conn.executemany("INSERT INTO {} VALUES (?,?,?,?,?,?)".format(table_name), a)

    def add_subdiv_rows(self, rows, table_name, timestamp):
        a = []
        for r in rows:
            a.append((
                timestamp,  #timestamp
                int(r[0]),              #rank
                r[1],                   #subdiv
                float(r[2])             #score
            ))
        self.conn.executemany("INSERT INTO {} VALUES (?,?,?,?)".format(table_name), a)

    def add_country_rows(self, rows, table_name, timestamp):
        a = []
        for r in rows:
            a.append((
                timestamp,  #timestamp
                int(r[0]),              #rank
                r[1],                   #country
                int(r[2]),              #users
                int(r[3]),              #unis
                float(r[4])             #score
            ))
        self.conn.executemany("INSERT INTO {} VALUES (?,?,?,?,?,?)".format(table_name), a)


    def add_data(self, global_uni, global_user, global_country, swe_tables, chalmers_user, time=None):
        if(time == None): time = datetime.now()
        time = int(time.timestamp())

        self.add_uni_rows(global_uni, "global_uni", time)
        self.add_user_rows(global_user, "global_user", time)
        self.add_country_rows(global_country, "global_country", time)

        self.add_uni_rows(swe_tables[0], "swe_uni", time)
        self.add_user_rows(swe_tables[1], "swe_user", time)
        self.add_subdiv_rows(swe_tables[2], "swe_subdiv", time)

        self.add_user_rows(chalmers_user, "chalmers_user", time)

        self.conn.commit()

    #use for extra sql-injection protection
    def is_table(self, table):
        return table in ['global_uni', 'global_user', 'global_country', 'swe_uni', 'swe_user', 'swe_subdiv', 'chalmers_user']

    def max_time(self, timestamp=False):
        #antar att åtminstone ett uni läggs till varje gång
        t = self.conn.execute("SELECT MAX(timestamp) FROM global_uni").fetchone()[0]
        if(t == None): return t
        if(timestamp): return t
        return datetime.fromtimestamp(t)

    def global_user_history(self, name):
        r = self.conn.execute("SELECT * from global_user WHERE name=?", (name,)).fetchall()
        return r

    def swe_user_history(self, name):
        r = self.conn.execute("SELECT * from swe_user WHERE name=?", (name,)).fetchall()
        return r

    def top_swe_unis(self):
        r = self.conn.execute("SELECT uni from swe_uni WHERE timestamp=?", (self.max_time().timestamp(),)).fetchall()
        return [x[0] for x in r]

    def swe_uni_history(self, uni):
        r = self.conn.execute("SELECT * from swe_uni WHERE uni=?", (uni,)).fetchall()
        return r

    def history(self, mintimestamp, type, names, place='all'):
        allowed_places = ['global']
        if(type in ['user', 'uni']): allowed_places.append('swe')
        if(type == 'user'): allowed_places.append('chalmers')

        if(place == 'all'):
            h = [self.history(mintimestamp, type, names, place) for place in allowed_places]
            r = [(name, [w for y in h for x in y for z in y if(z[0]==name) for w in z[1]]) for name in names]
            for _,x in r:
                x.sort()
                i = 1
                while(i < len(x)):
                    if(x[i][0]-x[i-1][0] < 3600):
                        x.pop(i)
                    else: i+=1
        else:
            table = place + "_" + type
            assert(self.is_table(table)) #sql-injection protection
            r = self.conn.execute("SELECT * from %s WHERE %s IN (%s) AND timestamp >= ?" % (table, 'name' if type=='user' else type, ','.join('?'*len(names))), names+[mintimestamp]).fetchall()
            r = [(name, [x for x in r if x[2]==name]) for name in names]
        return r

    def get_top(self, type, place, cnt):
        table = place + "_" + type
        if(not self.is_table(table)): return None
        time = self.max_time(True)
        r = self.conn.execute("SELECT %s,score from %s WHERE timestamp=?" % ('name' if type=='user' else type, table), (time,)).fetchall()
        r.sort(key=lambda x:-x[1]) # (unnecessary)
        r = r[:cnt]
        return [x[0] for x in r]

    def printall(self):
        for table in ["global_uni", "global_user", "global_country", "swe_uni", "swe_user", "swe_subdiv", "chalmers_user"]:
            print("----------", table, "----------")
            x = self.conn.execute("SELECT * from " + table).fetchall()
            x = [str(y) for y in x]
            print("\n".join(x))
            if(table != "chalmers_user"): print("\n")
