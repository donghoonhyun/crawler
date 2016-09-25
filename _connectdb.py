import MySQLdb


def db_connection():
    # Open database connection
    db = MySQLdb.connect(host="localhost", user="newuser", passwd="Newuser123!", db="newworld")
    db.set_character_set('utf8mb4')
    return db


def init_target_list():
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "UPDATE target SET needVisit = 'Y' WHERE needVisit = 'N'"

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def get_target_list():
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "SELECT targetID, targetNameASCII FROM target WHERE needVisit = 'Y' ORDER BY targetID"

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Fetch all the rows in a list
        results = cursor.fetchall()
        target = []
        for row in results:
            data = [row[0], row[1]]
            target.append(data)

        return target
    except Exception as e:
        print(str(e))

    # Disconnect from database
    db.close()


def get_url_list():
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "SELECT targetID, url FROM url WHERE isVisited = 'N' ORDER BY url"

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Fetch all the rows in a list
        results = cursor.fetchall()
        url_info = []
        for row in results:
            data = [row[0], row[1]]
            url_info.append(data)

        return url_info
    except Exception as e:
        print(str(e))

    # Disconnect from database
    db.close()


def get_document():
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "SELECT targetID, url, concat(title, article) doc FROM document WHERE isAnalysed = 'N' ORDER BY targetID"

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Fetch all the rows in a list
        results = cursor.fetchall()

        return results
    except Exception as e:
        print(str(e))

    # Disconnect from database
    db.close()


def save_url(targetid, urls):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    try:
        for url in urls:
            # Prepare SQL query to INSERT a record into the database
            sql = "INSERT INTO url (targetID, url, collectDate) \
                    VALUES (%s, %s, curdate()+0)" % \
                  ("'"+str(targetid)+"'", "'"+url+"'")

            # Execute the SQL command
            cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        if str(e)[1:5] == '1062':  # Duplicate entry
            print('Targetid ' + targetid + ' has this url(' + url + ') already.')

        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def save_article(targetid, url, title, article, fromsite, wdate, writer, cnt):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "INSERT INTO document (targetID, url, title, article, fromSite, writeDate, writer, viewCount) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" % \
          ("'"+targetid+"'", "'"+url+"'", "'"+title+"'", "'"+article+"'", "'"+fromsite+"'", "'"+wdate+"'", "'"+writer
           + "'", "'"+cnt+"'")

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def save_word(targetid, url, df):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "INSERT INTO word (targetID, url, word, cnt) \
            VALUES (%(targetID)s, %(url)s, %(word)s, %(count)s)"

    params = [df.iloc[line, :].to_dict() for line in range(len(df))]

    try:
        # Execute the SQL command
        cursor.executemany(sql, params)

        # Commit changes in the database
        db.commit()
        mark_document(str(targetid), url)

    except MySQLdb.MySQLError as sqle:
        if str(sqle)[1:5] == '1062':  # Duplicate entry
            # Rollback in case there is any error
            db.rollback()

            # Prepare SQL query to INSERT a record into the database
            sql = "UPDATE document set isAnalysed = 'E' \
                        WHERE targetID = %s AND url = %s " % \
                  ("'" + str(targetid) + "'", "'" + url + "'")
            # Execute the SQL command
            cursor.execute(sql)

            # Commit changes in the database
            db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def mark_target(targetid):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "UPDATE target set needVisit = 'N',  visitDate = curdate()+0 \
            WHERE targetID = %s" % \
          ("'"+targetid+"'")

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def mark_url(targetid, url):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "UPDATE url set isVisited = 'Y', visitDate = curdate()+0 \
            WHERE targetID = %s AND url = %s" % \
          ("'"+targetid+"'", "'"+url+"'")

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


def mark_document(targetid, url):
    db = db_connection()

    # Prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database
    sql = "UPDATE document set isAnalysed = 'Y' \
            WHERE targetID = %s AND url = %s" % \
          ("'"+targetid+"'", "'"+url+"'")

    try:
        # Execute the SQL command
        cursor.execute(sql)

        # Commit changes in the database
        db.commit()

    except Exception as e:
        print(str(e))
        # Rollback in case there is any error
        db.rollback()

    # Disconnect from database
    db.close()


