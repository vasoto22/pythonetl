import psycopg2

class Database:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except psycopg2.Error as e:
            print(f"Error al conectar a la base de datos: {e}")
        return conn

    def test_connection(self):
        conn = self.connect()
        if conn is not None:
            print("Conexión exitosa a la base de datos.")
            conn.close()
            return True
        else:
            print("No se pudo conectar a la base de datos.")
            return False
    
# db = Database('localhost', 'etl', 'postgres', 'admin')
# db.test_connection()
