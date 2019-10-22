import mysql.connector
from mysql.connector import Error
import argparse
import json
from dict2xml import dict2xml as xmlify


def connection():
    try:
        conn = mysql.connector.connect(host=args.host,
                                       user=args.user,
                                       password=args.password)
        if conn.is_connected():
            return conn
    except Error as e:
        print(e)


def write_data(queries):
    try:
        conn = connection()
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(queries[query])
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def read_data(query):
    try:
        conn = connection()
        cursor = conn.cursor()
        cursor.execute(query)
    except Error as e:
        print(e)
    finally:
        return cursor.fetchall()
        cursor.close()
        conn.close()


def create_json(save_path, file_name, query_answer):
    with open(save_path + file_name, 'w') as f:
        json.dump(query_answer, f, indent=4, default=str)


def create_xml(save_path, file_name, query_answer):
    with open(save_path + file_name, "w") as f:
        f.write(xmlify(query_answer, wrap="all", indent="  "))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Working with students database')
    parser.add_argument('host', type=str, help='Enter host')
    parser.add_argument('user', type=str, help='Enter user')
    parser.add_argument('password', type=str, help='Enter password')
    parser.add_argument('database', type=str, help='Enter database')
    parser.add_argument('rooms_json', type=str, help='Enter path to rooms.json')
    parser.add_argument('students_json', type=str, help='Enter path to students.json')
    parser.add_argument('doc_type', type=str, help='Enter type of files to save')
    parser.add_argument('save_path', type=str, help='Enter path where to save answers')

    args = parser.parse_args()
    print(args.host, args.user, args.user)

    db_create_queries = {
        'create_schema': """CREATE SCHEMA IF NOT EXISTS `students` DEFAULT CHARACTER SET utf8 ;    """,
        'create_table_rooms': """CREATE TABLE IF NOT EXISTS `students`.`rooms` (
                                  `id` INT NULL DEFAULT NULL,
                                  `name` VARCHAR(45) NULL DEFAULT NULL)
                                ENGINE = InnoDB
                                DEFAULT CHARACTER SET = utf8;""",
        'create_table': """CREATE TABLE IF NOT EXISTS `students`.`students` (
                                  `id` INT(11) NULL DEFAULT NULL,
                                  `name` VARCHAR(45) NULL DEFAULT NULL,
                                  `birthday` DATETIME NULL DEFAULT NULL,
                                  `room` INT NULL DEFAULT NULL,
                                  `sex` VARCHAR(45) NULL DEFAULT NULL)
                                ENGINE = InnoDB
                                DEFAULT CHARACTER SET = utf8;""",
    }
    args.database = 'students'
    db_select_queries = {
        'count_students': (f""" select {args.database}.rooms.name, count(*)
                from  {args.database}.rooms join students.students
                on {args.database}.rooms.id = students.students.room
                GROUP BY {args.database}.students.room
                order by LENGTH({args.database}.rooms.name), {args.database}.rooms.name; """),
        'search_lowest_avg_age': f"""SELECT
                    {args.database}.rooms.name,
                      avg(
                        (YEAR(CURRENT_DATE) - YEAR({args.database}.students.birthday)) -
                        (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT({args.database}.students.birthday, '%m%d'))
                      ) AS age
                from  {args.database}.students join {args.database}.rooms
                on {args.database}.rooms.id = {args.database}.students.room
                group by {args.database}.rooms.name
                order by age limit 5;""",
        'search_max_difference': f"""SELECT
                  {args.database}.rooms.name,
                  max(
                  (
                    (YEAR(CURRENT_DATE) - YEAR({args.database}.students.birthday)) -
                    (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT({args.database}.students.birthday, '%m%d'))
                  )
                  ) -
                    min(
                  (
                    (YEAR(CURRENT_DATE) - YEAR({args.database}.students.birthday)) -
                    (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT({args.database}.students.birthday, '%m%d'))
                  )
                  ) as difference
                from  {args.database}.students join {args.database}.rooms
                on {args.database}.rooms.id = {args.database}.students.room
                group by {args.database}.rooms.name
                order by difference DESC limit 5;""",
        'search_different_sexes': f"""select {args.database}.rooms.name RoomName,  CONCAT({args.database}.students.sex, count(*))
                from {args.database}.students join {args.database}.rooms
                where {args.database}.students.room = {args.database}.rooms.id
                GROUP BY {args.database}.rooms.name, {args.database}.students.sex
                HAVING COUNT(*) >= 0
                order by LENGTH({args.database}.rooms.name), {args.database}.rooms.name, {args.database}.students.sex;""",
    }

    write_data(db_create_queries)

    with open(args.rooms_json + 'rooms.json', "r") as rooms_json_file:
        rooms_json = json.load(rooms_json_file)
        query_dict_rooms = {}
        for room in rooms_json:
            query = f"""INSERT INTO students.rooms (id, name)
                                   VALUES
                                   ({room['id']}, '{room['name']}') """
            query_dict_rooms.setdefault(room['id'], query)
    write_data(query_dict_rooms)

    with open(args.students_json + 'students.json', "r") as students_json_file:
        students_json = json.load(students_json_file)
        query_dict_students = {}
        for stud in students_json:
            query = f"""INSERT INTO students.students (id, name, birthday, room, sex)
                        VALUES
                        ({stud["id"]}, '{stud["name"]}', '{stud["birthday"]}', {stud["room"]}, '{stud["sex"]}')"""
            query_dict_students.setdefault(stud['id'], query)
    write_data(query_dict_students)

    count_students = read_data(db_select_queries['count_students'])
    lowest_avg_age = read_data(db_select_queries['search_lowest_avg_age'])
    max_difference = read_data(db_select_queries['search_max_difference'])
    different_sexes = read_data(db_select_queries['search_different_sexes'])

    if args.doc_type == 'json':
        create_json(args.save_path, 'query1.json', count_students)
        create_json(args.save_path, 'query2.json', lowest_avg_age)
        create_json(args.save_path, 'query3.json', max_difference)
        create_json(args.save_path, 'query4.json', different_sexes)
    else:
        create_xml(args.save_path, 'query1.xml', count_students)
        create_xml(args.save_path, 'query2.xml', lowest_avg_age)
        create_xml(args.save_path, 'query3.xml', max_difference)
        create_xml(args.save_path, 'query4.xml', different_sexes)