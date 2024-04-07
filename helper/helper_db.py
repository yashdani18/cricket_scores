import mysql.connector

from config.mysql import HOST, USER, PASSWORD, DATABASE


def init_db():
    db = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

    cursor = db.cursor()
    return db, cursor


def close_db(db, cursor):
    cursor.close()
    db.close()
