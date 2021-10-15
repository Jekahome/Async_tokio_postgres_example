use tokio_postgres::{Client, Statement, NoTls, Error, Connection, Row, Config};
use tokio_postgres::types::{Type, ToSql, FromSql}; 
use tokio_postgres::tls::{NoTlsStream};
use tokio_postgres_example::settings;
use my_type_safety::{Id};

// Все примеры https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/main.rs
 
#[tokio::main] // По умолчанию tokio_postgres использует crate tokio в качестве runtime (среды выполнения).
async fn main() -> std::result::Result<(), tokio_postgres::Error> { 
    // Connect to the database. https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/runtime.rs
    // DATABASE_URL=postgres://rust:job_queue@localhost:5432/rust cargo run
    /*
        // Конфиг через String
        let config:String = settings::config().expect("Error parse config");
        let (mut client, connection):(Client,Connection<_,_>) =
            tokio_postgres::connect(&config, NoTls).await?;
    */

    // Конфиг через tokio_postgres::Config
    let config:Config = settings::config_2().expect("Error configure");
    let  (mut client,mut connection):(Client,Connection<tokio_postgres::Socket, NoTlsStream>)  = config.connect(  NoTls).await?;

    // Объект подключения выполняет фактическую связь с базой данных, поэтому запускает его самостоятельно.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    /*
        // SslConnector
        [dependencies]
        postgres = "0.18.1"
        openssl = "0.10.32"
        postgres-openssl = "0.4.0"
        
        use openssl::ssl::{SslConnector, SslMethod};
        use postgres_openssl::MakeTlsConnector;
        let builder = SslConnector::builder(SslMethod::tls()).unwrap();
        let tls_connector = MakeTlsConnector::new(builder.build());
        let url = format!("host={} port=5432 user={} password={} dbname={} sslmode=require",pg_host, pg_user, pg_password, pg_dbname);
        let mut pg_client = postgres::Client::connect(&url, tls_connector).expect("failed to connect to postgres");
    */


    // Client: https://docs.rs/postgres/0.19.0/postgres/struct.Client.html#method.query_one
    // Связанный запрос не отправляется в базу данных до тех пор, пока не будет сначала опрошено будущее, возвращаемое методом.
    // Запросы выполняются в том порядке, в котором они были впервые опрошены, а не в том порядке, в котором создаются их фьючерсы

    // query ----------------------------------------------------------------------------------------------------------------------------------

     query_example(&client).await?;

    // error
     error_example(&client).await?;  

    // transaction ----------------------------------------------------------------------------------------------------------------------------
     transaction_example(&mut client).await?;

    // execute --------------------------------------------------------------------------------------------------------------------------------

     execute_example(&client).await?;

     drop_create_table(&client).await;

     insert_data(&client).await;

    // notifications --------------------------------------------------------------------------------------------------------------------------
     notifications_example().await?;
    
    Ok(())
}

/// Implementations ToSql and FromSql traits
mod my_type_safety {

    use bytes::{BufMut, BytesMut};
    use tokio_postgres::types::{Kind, IsNull, Type, ToSql, FromSql}; 
    use std::io::Read;
    use byteorder::{BigEndian, ByteOrder, ReadBytesExt};

    #[derive(Debug,PartialEq)]
    pub struct Id(pub i32);
    
    /// Example:
    /// 
    /// let mut buf = BytesMut::new();
    /// Id(11).to_sql(&Type::INT4,&mut buf );
    /// assert_eq!(&[0x0, 0x0, 0x0, 0xB],&buf[..]);
    /// assert_eq!(b"\0\0\0\x0b",&buf[..]);
    impl ToSql for Id {
        fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
            
            <&i32 as ToSql>::to_sql(&&self.0, ty, w) 
        }
        
        fn accepts(ty: &Type) -> bool
        where
            Self: Sized{
                matches!(*ty,Type::INT4)
        }

        tokio_postgres::types::to_sql_checked!();
    }
    
    /// Example:
    /// 
    /// let mut buf = BytesMut::new();
    /// buf.put_i32(11);// 11 == b"\0\0\0\x0b"
    /// assert_eq!(Id(11),Id::from_sql(&Type::INT4,&buf[..]).unwrap());
    /// assert_eq!(Id(11),Id::from_sql(&Type::INT4,&[0x0, 0x0, 0x0, 0xB]).unwrap());
    impl<'a> FromSql<'a> for Id {
        fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, std::boxed::Box<dyn std::error::Error + Sync + Send>> {  

            int4_from_sql(raw)
            .and_then(|id|{
                Ok(Id(id))
            })
    // match std::str::from_utf8(raw){Ok(id) => id.parse::<i32>().and_then(| id | Ok(Id(id)) ).map_err(| err | err.into() ),Err(err) => Err( err.into() )}  
        }

        fn accepts(ty: &Type) -> bool {
            matches!(*ty,Type::INT4)
        } 
    }

    // Функции из библиотеки rust-postgres
    // https://github.com/sfackler/rust-postgres/blob/fc10985f9fdf0903893109bc951fb5891539bf97/postgres-protocol/src/types/mod.rs#L97

    /// Serializes an `INT4` value.
    #[inline]
    pub fn int4_to_sql(v: i32, buf: &mut BytesMut) {
        buf.put_i32(v);
    }

    /// Deserializes an `INT4` value.
    #[inline]
    pub fn int4_from_sql(mut buf: &[u8]) -> Result<i32, std::boxed::Box<dyn std::error::Error + Sync + Send>> {
        let v = buf.read_i32::<BigEndian>()?;
        if !buf.is_empty() {
            return Err("invalid buffer size".into());
        }
        Ok(v)
    }
 }

async fn transaction_example(client: &mut Client) -> std::result::Result<(), tokio_postgres::Error>{
    // Требует mut client

    // По умолчанию транзакция откатится - используйте commit метод для ее фиксации

    // build_transaction
    // let mut transaction = client.build_transaction().isolation_level(tokio_postgres::IsolationLevel::RepeatableRead).start().await?;
    // let mut transaction = client.build_transaction().isolation_level(IsolationLevel::Serializable).read_only(true).deferrable(true).start().await?;

    let mut transaction = client.transaction().await?;
    let stmt = transaction.prepare_typed("UPDATE todo SET name = $2 WHERE id = $1", &[Type::INT4,Type::VARCHAR]).await?;
    transaction.execute(&stmt, &[&Id(1),&"pet my cat 2"]).await?;
    // ...
    transaction.commit().await?;
    //transaction.rollback().await?;


    //-------------------------------------------------------------------------
    let _:Vec<tokio_postgres::SimpleQueryMessage> = client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (id INT);
             INSERT INTO foo (id) VALUES (1), (2), (3);",
        )
        .await?;

    let transaction = client.transaction().await?;

    let portal = transaction
        .bind("SELECT * FROM foo ORDER BY id", &[])
        .await?;

    let rows = transaction.query_portal(&portal, 2).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);

    let rows = transaction.query_portal(&portal, 2).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 3);
    transaction.rollback().await?;

    Ok(())
}

async fn query_example(client: &Client) -> std::result::Result<(), tokio_postgres::Error>{
    // https://docs.rs/postgres/0.19.0/postgres/struct.Client.html#method.query_one

    // query - выборка множества строк, возвращает Vec<Row>
    // query_one - выборка одной строки, возвращает Row
    // query_opt - выборка одной строки, возвращает Option<Row>
    // query_raw - выборка множества строк, принирмает итератор параметров и возвращает результат итератором
    // simple_query - выборка множества строк, использует строку как SQL запрос без поддержки prepare и биндинга данных

    // query
    let rows = client.query("SELECT $1::TEXT", &[&"hello world"]).await?;
    let value: &str = rows[0].get(0);
    assert_eq!(value, "hello world");
    
    // query
    let id:i32  = 2;
    let rows:Vec<Row> = client.query("SELECT name FROM todo WHERE id = $1::INT4", &[&id]).await?;
    let value: &str = rows[0].get(0);
    println!("Result prepare: {}",value);
    assert_eq!(value, "pet my dog");

    // used type safety
    let id:Id  = Id(2);
    let rows:Vec<Row> = client.query("SELECT name FROM todo WHERE id = $1::INT4", &[&id]).await?;
    let value: &str = rows[0].get(0);
    println!("Result prepare: {}",value);
    assert_eq!(value, "pet my dog");

    // query_one
    let id:Id  = Id(2);
    let row:Row = client.query_one("SELECT id,name FROM todo WHERE id = $1::INT4", &[&id]).await?;
    let id1:Id = row.get(0);
    let id2:Id = row.get("id");
    let name1: &str = row.get(1);
    let name2: &str = row.get("name");
    println!("Result prepare: id:{} {:?}, name: {} {}",id1.0,id2,name1,name2);
    assert_eq!(name1, "pet my dog");

    // query_opt
    let stmt = client.prepare_typed( "SELECT id,name FROM todo WHERE id = $1::INT4", &[Type::INT4]).await?;
    let id:Id  = Id(200);
    let row = client.query_opt(&stmt, &[&id]).await?;
    match row {
        Some(row) => {
            let name: i32 = row.get("name");
            println!("name: {}", name);
        }
        None => println!("no matching name"),
    }
 
    // query_raw
    // Он принимает итератор параметров, а не срез, и возвращает итератор строк, а не собирает их в массив.
    // Применение, реализовать для структуры данных итератор и передавать вместо среза параметров эту структуру
    use futures::{pin_mut, TryStreamExt};
    use tokio_postgres::RowStream;
    let stmt = client.prepare_typed( "SELECT id,name FROM todo WHERE id = $1::INT4 AND id > $2::INT4", &[Type::INT4,Type::INT4]).await?;
    let ids:Vec<i32> = vec![2,0];
    let mut res_it:RowStream = client.query_raw(&stmt, ids).await?;
    pin_mut!(res_it);
    while let Some(row) = res_it.try_next().await? {
        let id: Id = row.get("id");
        let name: String = row.get("name");
        println!("id: {}, name: {}", id.0,name);
    }

    // simple_query 
    // использует raw SQL запрос без поддержки prepare и биндинга данных
    // Выполняет последовательность операторов SQL
    let res:Vec<tokio_postgres::SimpleQueryMessage> = client
        .simple_query(
            "SELECT id,name FROM todo WHERE id = 1;
             SELECT id,name FROM todo WHERE id = 2",
        )
        .await?;

        for item in res {
            match item {
                tokio_postgres::SimpleQueryMessage::Row(simple) => {
                    // Row len:2, value:Some("pet my cat")
                    // Row len:2, value:Some("pet my dog")
                    println!("Row len:{}, value:{:?}",simple.len(),simple.get("name"));
                },
                tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                    println!("CommandComplete {:?}",n);
                },
                _ => {println!("SimpleQueryMessage not found");}
            }
        }
    /*
        let messages = client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('steven'), ('joe');
            SELECT * FROM foo ORDER BY id;",
        )
        .await
        .unwrap();
     */
     

    // prepare_typed
    let stmt = client.prepare_typed(
        "SELECT id,name FROM todo WHERE id = $1::INT4", 
    &[Type::INT4]).await.expect("failed to create prepared statement");
    let rows = client.query(&stmt,&[&id]).await?;
    let id:Id = rows[0].get(0);
    let name:&str = rows[0].get(1);
    println!("id: {:?}, name: {:?}",id,name);

    // prepare
    let stmt = client.prepare("SELECT id,name FROM todo WHERE id = $1::INT4").await.expect("failed to create prepared statement");
    let id:Id  = Id(2);
    let rows = client.query(&stmt,&[&id]).await?;
    let id:Id = rows[0].get(0);
    let name:&str = rows[0].get(1);
    println!("id: {:?}, name: {:?}",id,name);
    
    Ok(())
}

async fn execute_example(client: &Client) -> std::result::Result<(), tokio_postgres::Error>{
    // batch
    let (stmt1,stmt2) = pipelined_prepare(&client).await?;
    let id:Id  = Id(2);
    let rows = client.query(&stmt1,&[&id]).await?;
    let id:Id = rows[0].get(0);
    let name:&str = rows[0].get(1);
    println!("id: {:?}, name: {:?}",id,name);
    let id:Id  = Id(4);
    let name = "bla my dog";
    let count = client.execute(&stmt2,&[&id,&name]).await?;
    println!("count: {:?}",count);

    // batch_execute
    // Выполняет последовательность операторов SQL
    match client.batch_execute(
        "CREATE TABLE IF NOT EXISTS foo (id serial PRIMARY KEY);
               DROP TABLE foo125;").await {
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) => {
            println!("Error 42P01");
        }
        t => panic!("unexpected return: {:?}", t),
    }
    match client.batch_execute("SELECT pg_sleep(100)").await {
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) => {}
        t => panic!("unexpected return: {:?}", t),
    }
    
    // execute
    let stmt = client.prepare_typed(
        "DELETE FROM todo WHERE id = $1::INT4", 
    &[Type::INT4]).await.expect("failed to create prepared statement");
    let id:Id  = Id(4);
    let count = client.execute(&stmt,&[&id]).await?;
    println!("count delete: {:?}",count);

    // used tokio-postgres with features = ["with-chrono-0_4"]
    use chrono::{DateTime,Utc,Local};
    let utc: DateTime<Local> = Utc::now().with_timezone(&Local);
    let stmt = client.prepare_typed(
        "UPDATE todo SET checked_date = $1::TIMESTAMP WITH TIME ZONE WHERE id = $2::INT4", 
    &[Type::TIMESTAMPTZ,Type::INT4]).await.expect("failed to create prepared statement");
    let id:Id  = Id(3);
    let count = client.execute(&stmt,&[&utc,&id]).await?;
    println!("count update: {:?}",count);

    Ok(())
}

async fn pipelined_prepare( client: &Client) -> Result<(Statement, Statement), Error>{
    // https://docs.rs/tokio-postgres/0.7.3/tokio_postgres/index.html#pipelining
    // Конвейерная обработка может повысить производительность в тех случаях, когда необходимо выполнить несколько независимых запросов.
    // Конвейерная обработка позволяет клиенту отправлять все запросы на сервер заранее, сервер PostgreSQL выполняет запросы последовательно - 
    // конвейерная обработка просто позволяет обеим сторонам соединения работать одновременно, когда это возможно.
    // Конвейерная обработка происходит автоматически при одновременном опросе фьючерсов (например, с помощью join комбинатора фьючерсов ):

    use futures::future;
    use std::future::Future;

    future::try_join(
        client.prepare("SELECT id,name FROM todo WHERE id = $1::INT4"),
        client.prepare("INSERT INTO todo (id, name) VALUES ($1, $2)")
    ).await

    // также можно отправить в try_join! и execute,query и т.д.
    /*
        let insert = client.prepare("INSERT INTO foo (name) VALUES ($1), ($2)");
        let select = client.prepare("SELECT id, name FROM foo ORDER BY id");
        let (insert, select) = try_join!(insert, select).unwrap();

        let insert = client.execute(&insert, &[&"alice", &"bob"]);
        let select = client.query(&select, &[]);
        let (_, rows) = try_join!(insert, select).unwrap();
    */
}

async fn drop_create_table(client: &Client) {
    let res = client.execute("DROP TABLE IF EXISTS inventory;", &[]).await;
    match res {
        Ok(_) => println!("dropped table"),
        Err(e) => println!("failed to drop table {}", e),
    }
    let res = client
        .execute("CREATE TABLE IF NOT EXISTS inventory (id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER);", &[])
        .await;
    match res {
        Ok(_) => println!("created table"),
        Err(e) => println!("failed to create 'inventory' table, {}", e),
    }
}

async fn insert_data(client: &Client) {

    let stmt = client
        .prepare_typed("INSERT INTO inventory (name, quantity) VALUES ($1, $2) RETURNING id;", &[Type::VARCHAR, Type::INT4]).await
        .expect("failed to create prepared statement");

    // вариант с query_one (возвращает результат запроса RETURNING)    
    let row = client
        .query_one(&stmt, &[&"item-1", &42]).await
        .expect("insert failed");

    let id: i32 = row.get(0);
    println!("inserted item with id {}", id);

    // вариант с execute (возвращает количество строк)
    let count = client
        .execute(&stmt, &[&"item-2", &43]).await
        .expect("insert failed");
    println!("count {}", count);


    // несколько вставок
    let mut id: i32;
    let mut row: Row;
    for item in 0..3{
        row = client
             .query_one(&stmt, &[&format!("item-{}",&item), &item]).await
             .expect("insert failed");
        id = row.get(0);
        println!("inserted item with id {}", id);
    }
}

async fn error_example(client: &Client) -> Result<(), Error>{
    // postgres::error::Error
    // postgres::error::SqlState
    
    // batch_execute - Выполняет последовательность операторов SQL
    match client.batch_execute(
        "CREATE TABLE IF NOT EXISTS foo (id serial PRIMARY KEY);
               DROP TABLE foo125;").await {
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) => {
            println!("Error 42P01");
        }
        t => {panic!("unexpected return: {:?}", t)}
    }
    match client.batch_execute("SELECT pg_sleep(100)").await {
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) => {
            println!("Error 57014");
        }
        t => {panic!("unexpected return: {:?}", t)}
    }
    Ok(())
}


//-----------------------------------------------------------------------------------------------------------

use tokio::net::TcpStream;
use futures::FutureExt;
async fn notifications_example() -> std::result::Result<(), tokio_postgres::Error>{
    // https://github.com/sfackler/rust-postgres/blob/d45461614aca87022c17a2cc26b22325bf161fa5/tokio-postgres/tests/test/main.rs#L616
    // https://www.postgresql.org/docs/9.0/sql-listen.html
    // LISTEN virtual;
    // NOTIFY virtual;
    // Asynchronous notification "virtual" received from server process with PID 8448.
    use tokio_postgres::Notification;
    
    use futures::channel::mpsc;
    use futures::{
        future, join, pin_mut, stream, try_join, SinkExt, StreamExt, TryStreamExt,
    };
    
    use tokio_postgres::AsyncMessage;
    
    let (client, mut connection):(Client,Connection<TcpStream, NoTlsStream>) = connect_raw("host=localhost user=rust password='job_queue' dbname=rust").await.unwrap();
    let (tx, rx) = mpsc::unbounded();
    let stream =
        stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!("{}", e));
    let connection = stream.forward(tx).map(|r| r.unwrap());
    tokio::spawn(connection);

    client
        .batch_execute(
            "LISTEN test_notifications;
            NOTIFY test_notifications, 'hello';
            NOTIFY test_notifications, 'world';",
        )
        .await
        .unwrap();

    drop(client);

    let notifications = rx
        .filter_map(|m| match m {
            AsyncMessage::Notification(n) => future::ready(Some(n)),
            _ => future::ready(None),
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].channel(), "test_notifications");
    assert_eq!(notifications[0].payload(), "hello");
    assert_eq!(notifications[1].channel(), "test_notifications");
    assert_eq!(notifications[1].payload(), "world");

    Ok(())
}
async fn connect_raw(s: &str) -> Result<(Client, Connection<TcpStream, NoTlsStream>), Error> {
    let socket = TcpStream::connect("127.0.0.1:5432").await.unwrap();
    let config = s.parse::<Config>().unwrap();
    config.connect_raw(socket, NoTls).await
}

async fn connect(s: &str) -> Client {
    let (client, connection) = connect_raw(s).await.unwrap();
    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);
    client
}
//-----------------------------------------------------------------------------------------------------------
