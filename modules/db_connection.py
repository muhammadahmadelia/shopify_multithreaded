import pymysql as MySQLdb

class Connection:
    def __init__(self) -> None:
        self.__server = ''
        self.__port = ''
        self.__username = ''
        self.__password = ''
        self.__database = ''
        pass

    @property
    def server(self) -> str:
        return self.__server

    @server.setter
    def server(self, server: str) -> None:
        self.__server = server

    @property
    def port(self) -> str:
        return self.__port

    @port.setter
    def port(self, port: str) -> None:
        self.__port = port

    @property
    def username(self) -> str:
        return self.__username

    @username.setter
    def username(self, username: str) -> None:
        self.__username = username

    @property
    def password(self) -> str:
        return self.__password

    @password.setter
    def password(self, password: str) -> None:
        self.__password = password

    @property
    def database(self) -> str:
        return self.__database

    @database.setter
    def database(self, database: str) -> None:
        self.__database = database


class DB_Connection:
    def __init__(self, DEBUG: bool) -> None:
        self.DEBUG = DEBUG
        self.__connection_obj = Connection()
        self.__db_connection = None
        pass
    
    def setting_connection_values(self, conn_string: str) -> None:
        try:
            db_con = conn_string.split(';')
            self.__connection_obj.server = db_con[0].split('=')[1]
            self.__connection_obj.port = db_con[1].split('=')[1]
            self.__connection_obj.username = db_con[2].split('=')[1]
            self.__connection_obj.password = db_con[3].split('=')[1]
            self.__connection_obj.database = db_con[4].split('=')[1]
        except Exception as e:
            if self.DEBUG: print('Exception in setting_connection_values: '+ str(e))
            else: pass

    def get_db_connection(self):
        try:
            if not self.__db_connection:
                self.__db_connection = MySQLdb.connect(
                    host=self.__connection_obj.server, 
                    user=self.__connection_obj.username, 
                    passwd=self.__connection_obj.password, 
                    db=self.__connection_obj.database, 
                    charset='utf8mb4', cursorclass=MySQLdb.cursors.DictCursor
                )
        except Exception as e:
            if self.DEBUG: print('Exception in get_db_connection: '+ str(e))
        finally: return self.__db_connection