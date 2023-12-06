import mysql.connector

class Database:
    def __init__(self, host, username, password, database_name):
        self.db = mysql.connector.connect(
            host = host,
            user = username,
            password = password,
            database = database_name
        )

    def drop_database(self, database_name):
        cursor = self.db.cursor()
        cursor.execute("DROP DATABASE IF EXISTS {}".format(database_name))
    
    def create_database(self, database_name):
        cursor = self.db.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

    def use_database(self, database_name):
        cursor = self.db.cursor()
        cursor.execute("USE {}".format(database_name))

    def create_table(self, table_name, table_columns):
        cursor = self.db.cursor()
        # join table columns togheter with , 
        columns = ", ".join(table_columns)
        cursor.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, columns))
        self.db.commit()

    def insert_into_table(self, table_name, table_columns, table_values):
        cursor = self.db.cursor()
        # join table columns togheter with , 
        columns = ", ".join(table_columns)
        # join table values togheter with , 
        values = ", ".join(table_values)
        cursor.execute("INSERT INTO {} ({}) VALUES ({})".format(table_name, columns, values))
        self.db.commit()

    def get_database_in_use(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT DATABASE()")
        return cursor.fetchone()


class Teacher:
    def __init__(self, teacher_id):
        self.id = teacher_id

    @staticmethod
    def create_teacher_table(database):
        table_columns = ["id INT PRIMARY KEY AUTO_INCREMENT"]
        database.create_table("teachers", table_columns)

    def insert_teacher(self, database):
        database.insert_into_table("teachers", ["id"], [str(self.id)])

    @staticmethod
    def drop_teacher_table(database):
        cursor = database.db.cursor()
        cursor.execute("DROP TABLE IF EXISTS teachers")
        
class StudentSOP:
    def __init__(self, student_id, diagnosis, resource_support = None, resource_center = None, teachers = []):
        self.id = student_id
        self.diagnosis = diagnosis
        self.resource_support = resource_support
        self.resource_center = resource_center
        self.teachers = teachers
    
    @staticmethod
    def create_student_table(database):
        table_columns = [
            "id INT PRIMARY KEY AUTO_INCREMENT",
            "diagnosis VARCHAR(255) NOT NULL",
            "resource_support VARCHAR(255)",
            "resource_center VARCHAR(255)"
        ]
        database.create_table("students_sop", table_columns)

    def insert_student(self, database):
        table_columns = ["id", "diagnosis"]
        table_values = [str(self.id), '"' + self.diagnosis + '"']
        if self.resource_support:
            table_columns.append("resource_support")
            table_values.append('"' + self.resource_support + '"')
        if self.resource_center:
            table_columns.append("resource_center")
            table_values.append('"' + self.resource_center + '"')
        
        database.insert_into_table("students_sop", table_columns, table_values)

        for teacher in self.teachers:
            STLinker.link_student_teacher(database, self.id, teacher.id)

    @staticmethod
    def drop_student_table(database):
        cursor = database.db.cursor()
        cursor.execute("DROP TABLE IF EXISTS students_sop")

class STLinker:
    @staticmethod
    def create_st_table(database):
        table_columns = [
            "id INT PRIMARY KEY AUTO_INCREMENT",
            "student_id INT NOT NULL",
            "teacher_id INT NOT NULL",
            "FOREIGN KEY (student_id) REFERENCES students_sop(id)",
            "FOREIGN KEY (teacher_id) REFERENCES teachers(id)"
        ]
        database.create_table("st_linker", table_columns)
    
    @staticmethod
    def link_student_teacher(database, student_id, teacher_id):
        table_columns = ["student_id", "teacher_id"]
        table_values = [str(student_id), str(teacher_id)]
        database.insert_into_table("st_linker", table_columns, table_values)

    @staticmethod
    def get_students_for_teacher(database, teacher_id):
        cursor = database.db.cursor()
        cursor.execute("SELECT students_sop.* FROM students_sop INNER JOIN st_linker ON students_sop.id = st_linker.student_id WHERE st_linker.teacher_id = {}".format(teacher_id))
        return cursor.fetchall()[::2]

    @staticmethod
    def drop_st_table(database):
        cursor = database.db.cursor()
        cursor.execute("DROP TABLE IF EXISTS st_linker")
