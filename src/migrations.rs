use tokio_postgres::{NoTls, Error,Client,Connection};
use refinery::config::Config;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");// Dir migrations
}

#[tokio::main] // По умолчанию tokio_postgres использует crate tokio в качестве runtime (среды выполнения).
async fn main() -> std::result::Result<(), tokio_postgres::Error> {
    // Connect to the database.
    // "postgres://rust:job_queue@localhost:5432/rust"
    let (mut client, connection):(Client,Connection<_,_>) =
        tokio_postgres::connect("host=localhost user=rust port=5432 password='job_queue' dbname=rust", NoTls).await?;

    // Объект подключения выполняет фактическую связь с базой данных, поэтому запускает его самостоятельно.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    drop_migrations_table(&mut client).await.expect("can run clean DB: {}");
    run_migrations(&mut client).await.expect("can run DB migrations: {}");
 
    Ok(())
}

// type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
async fn run_migrations(client:&mut Client) -> std::result::Result<(), refinery::Error> {
    println!("Running DB migrations...");
     
    /*let migration_report:Result<refinery::Report,  refinery::Error> = embedded::migrations::runner()
        .run_async(client)
        .await?;*/

    match embedded::migrations::runner()
    .run_async(client)
    .await {
        Ok(migration_report) => {
            for migration in migration_report.applied_migrations() {
                println!("Migration Applied - Name: {}, Version: {}", migration.name(), migration.version());
            }
        },
        Err(err) => {
            use std::error::Error;

             println!("Error: {:?}",err);
            // refinery::Error
            if let Some(err) = err
                .source()
                .map(|source| source.downcast_ref::<refinery::Error>())
            {
                // handle err
                 println!("Handle error: {:?}",err);
            }else{
                println!("Show error: {:?}",err);
            }
        }
    }
    println!("DB migrations finished!");
    Ok(())
}

 
async fn drop_migrations_table(client:&mut Client) -> std::result::Result<(), tokio_postgres::Error> {
    client.execute("DROP TABLE IF EXISTS refinery_schema_history", &[]).await?;
    Ok(())
}

 