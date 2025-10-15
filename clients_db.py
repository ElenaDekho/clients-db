import psycopg2

# === 1. Функция: создаёт структуру БД ===
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
                phone VARCHAR(20)
            );
        """)
        conn.commit()

# === 2. Добавить нового клиента ===
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                cur.execute("INSERT INTO phones (client_id, phone) VALUES (%s, %s);", (client_id, phone))
        conn.commit()
        return client_id

# === 3. Добавить телефон существующему клиенту ===
def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO phones (client_id, phone) VALUES (%s, %s);", (client_id, phone))
        conn.commit()

# === 4. Изменить данные клиента ===
def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        if first_name is not None:
            cur.execute("UPDATE clients SET first_name = %s WHERE id = %s;", (first_name, client_id))
        if last_name is not None:
            cur.execute("UPDATE clients SET last_name = %s WHERE id = %s;", (last_name, client_id))
        if email is not None:
            cur.execute("UPDATE clients SET email = %s WHERE id = %s;", (email, client_id))
        conn.commit()

# === 5. Удалить телефон клиента ===
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM phones WHERE client_id = %s AND phone = %s;", (client_id, phone))
        conn.commit()

# === 6. Удалить клиента ===
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clients WHERE id = %s;", (client_id,))
        conn.commit()

# === 7. Найти клиента по данным ===
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        conditions = []
        params = []
        if first_name is not None:
            conditions.append("c.first_name = %s")
            params.append(first_name)
        if last_name is not None:
            conditions.append("c.last_name = %s")
            params.append(last_name)
        if email is not None:
            conditions.append("c.email = %s")
            params.append(email)
        if phone is not None:
            conditions.append("p.phone = %s")
            params.append(phone)

        # Если нет условий — ищем всех
        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        cur.execute(f"""
            SELECT 
                c.id,
                c.first_name,
                c.last_name,
                c.email,
                ARRAY_AGG(p.phone) FILTER (WHERE p.phone IS NOT NULL)
            FROM clients c
            LEFT JOIN phones p ON c.id = p.client_id
            WHERE {where_clause}
            GROUP BY c.id, c.first_name, c.last_name, c.email;
        """, params)
        return cur.fetchall()

# === ОСНОВНОЙ КОД: создаём БД и тестируем всё ===
if __name__ == '__main__':
    # Шаг 1: Создаём базу clients_db (подключаясь к служебной 'postgres')
    conn_admin = psycopg2.connect(database="postgres", user="postgres", password="strekoza")
    conn_admin.autocommit = True
    with conn_admin.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS clients_db;")
        cur.execute("CREATE DATABASE clients_db;")
    conn_admin.close()

    # Шаг 2: Подключаемся к нашей базе
    with psycopg2.connect(database="clients_db", user="postgres", password="strekoza") as conn:
        # Создаём таблицы
        create_db(conn)

        # Добавляем клиентов
        id1 = add_client(conn, "Алёна", "Смирнова", "alena@example.com", ["+79001112233"])
        id2 = add_client(conn, "Иван", "Петров", "ivan@example.com")  # без телефона
        id3 = add_client(conn, "Сергей", "Павлов", "pavlov@example.com", ["+79553459874"])

        print("Все клиенты:")
        print(find_client(conn))

        # Добавляем телефон Ивану
        add_phone(conn, id2, "+79005556677")
        print("\nПосле добавления телефона Ивану:")
        print(find_client(conn, first_name="Иван"))

        # Добавляем второй телефон Ивану
        add_phone(conn, id2, "+79005523167")
        print("\nПосле добавления второго телефона Ивану:")
        print(find_client(conn, first_name="Иван"))

        print("\nВсе клиенты:")
        print(find_client(conn))

        # Меняем фамилию Алёны
        change_client(conn, id1, last_name="Иванова")
        print("\nПосле смены фамилии:")
        print(find_client(conn, first_name="Алёна"))

        # Добавим второго Ивана
        id4 = add_client(conn, "Иван", "Сидоров", "ivan2@example.com", ["+79112223344"])
        print("\nПосле добавления второго Ивана:")
        print(find_client(conn, first_name="Иван"))

        # Меняем почту Сергея
        change_client(conn, id3, email="pavlovserg@example.com")
        print("\nПосле смены почты Сергея:")
        print(find_client(conn, first_name="Сергей"))

        # Удаляем телефон
        delete_phone(conn, id2, "+79005556677")
        print("\nПосле удаления телефона у Ивана:")
        print(find_client(conn, first_name="Иван"))

        # Удаляем второй телефон Ивана
        delete_phone(conn, id2, "+79005523167")
        print("\nПосле удаления второго телефона у Ивана:")
        print(find_client(conn, first_name="Иван"))

        # Ищем клиента по имени
        print("\nПо имени нашёлся клиент:")
        print(find_client(conn, first_name="Алёна"))

        # Ищем клиента по фамилии
        print("\nПо фамилии нашёлся клиент:")
        print(find_client(conn, last_name="Павлов"))

        # Ищем клиента по email
        print("\nПо email нашёлся клиент:")
        print(find_client(conn, email="ivan@example.com"))

        # Ищем клиента по номеру телефона
        print("\nПо номеру телефона нашёлся клиент:")
        print(find_client(conn, phone="+79001112233"))

        # Удаляем клиента
        delete_client(conn, id2)
        print("\nПосле удаления Ивана:")
        print(find_client(conn))

    print("\n✅ Всё работает!")