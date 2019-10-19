import json
import mysql.connector
from dict2xml import dict2xml as xmlify
from mysql.connector import Error


def connection(host, database, user, password):
    try:
        conn = mysql.connector.connect(host=host,
                                       database=database,
                                       user=user,
                                       password=password)
        if conn.is_connected():
            return conn
    except Error as e:
        print(e)


def commit_query(query):
    try:
        conn = connection(host, database, user, password)
        cursor = conn.cursor()
        cursor.execute(query)
    except Error as e:
        print(e)
    finally:
        return cursor.fetchall()
        cursor.close()
        conn.close()


def count_students_in_rooms():
    query = f""" select rooms.name, count(*)
                from  rooms join students
                on rooms.id = students.room
                GROUP BY students.room 
                order by LENGTH(rooms.name), rooms.name; """
    commit_query(query)


def search_lowest_avg_age():
    query = f"""SELECT
                    rooms.name, 
                      avg(
                        (YEAR(CURRENT_DATE) - YEAR(students.birthday)) -                             
                        (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(students.birthday, '%m%d')) 
                      ) AS age 
                from  students join rooms
                on rooms.id = students.room
                group by rooms.name
                order by age limit 5;"""
    commit_query(query)


def search_max_difference():
    query = f"""SELECT
                  rooms.name, 
                  max(
                  (
                    (YEAR(CURRENT_DATE) - YEAR(students.birthday)) -                             
                    (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(students.birthday, '%m%d')) 
                  ) 
                  ) -
                    min(
                  (
                    (YEAR(CURRENT_DATE) - YEAR(students.birthday)) -                             
                    (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(students.birthday, '%m%d')) 
                  ) 
                  ) as difference 
                from  students join rooms
                on rooms.id = students.room
                group by rooms.name
                order by difference DESC limit 5;"""
    commit_query(query)


def search_different_sexes():
    query = f"""select rooms.name RoomName,  CONCAT(students.sex, count(*))
                from students join rooms
                where students.room = rooms.id
                GROUP BY rooms.name, students.sex
                HAVING COUNT(*) >= 0
                order by LENGTH(rooms.name), rooms.name, students.sex;"""
    commit_query(query)


def insert_room(rooms_json):
    try:
        conn = connection(host, database, user, password)
        cursor = conn.cursor()
        for room in rooms_json:
            query = f"""INSERT INTO rooms (id, name) 
                                   VALUES 
                                   ({room['id']}, '{room['name']}') """
            cursor.execute(query)
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def insert_student(students_json):
    try:
        conn = connection(host, database, user, password)
        cursor = conn.cursor()
        for stud in students_json:
            query = f"""INSERT INTO students (id, name, birthday, room, sex)
                            VALUES
                            ({stud["id"]}, '{stud["name"]}', '{stud["birthday"]}', {stud["room"]}, '{stud["sex"]}')"""
            cursor.execute(query)
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def load_and_insert_rooms_json(rooms_path):
    with open(rooms_path, "r") as rooms_json_file:
        rooms_json = json.load(rooms_json_file)
        insert_room(rooms_json)


def load_and_insert_students_json(students_path):
    with open(students_path, "r") as students_json_file:
        students_json = json.load(students_json_file)
        insert_student(students_json)


def create_json(save_path, file_name, query_function):
    with open(save_path + file_name, 'w') as f:
        json.dump(query_function, f, indent=4, default=str)


def create_xml(save_path, file_name, query_function):
    with open(save_path + file_name, "w") as f:
        f.write(xmlify(query_function, wrap="all", indent="  "))


if __name__ == '__main__':
    host = input("Enter host: ")
    database = input("Enter database: ")
    user = input("Enter user: ")
    password = input("Enter password: ")
    rooms_path = input('Enter your path to rooms.json: ')
    rooms_path += "/rooms.json"
    students_path = input('Enter your path to students.json: ')
    students_path += "/students.json"
    doc_type = input('Enter the type of file: ')
    path_to_save = input('Enter your path where to save query file: ')

    load_and_insert_rooms_json(rooms_path)
    load_and_insert_students_json(students_path)

    if doc_type == 'json':
        create_json(path_to_save, 'query1.json', count_students_in_rooms())
        create_json(path_to_save, 'query2.json', search_lowest_avg_age())
        create_json(path_to_save, 'query3.json', search_max_difference())
        create_json(path_to_save, 'query4.json', search_different_sexes())
    else:
        create_xml(path_to_save, 'query1.xml', count_students_in_rooms())
        create_xml(path_to_save, 'query2.xml', search_lowest_avg_age())
        create_xml(path_to_save, 'query3.xml', search_max_difference())
        create_xml(path_to_save, 'query4.xml', search_different_sexes())
