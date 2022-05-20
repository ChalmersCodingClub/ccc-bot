import sqlite3


class UserDbConn:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.create_realname_table()

    def create_realname_table(self):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS realname ('
            'discord_id   TEXT PRIMARY KEY,'
            'realname     TEXT'
            ')'
        )

    def remove_realname(self, discord_id):
        self.conn.execute("DELETE FROM realname WHERE discord_id=?", (discord_id,))
        self.conn.commit()

    def set_realname(self, discord_id, username):
        self.conn.execute("INSERT OR REPLACE INTO realname VALUES (?,?)", (discord_id, username))
        self.conn.commit()

    def get_realname(self, discord_id):
        s = self.conn.execute("SELECT realname from realname WHERE discord_id=?", (discord_id,)).fetchall()
        if(len(s) == 0): return None
        return s[0][0]

