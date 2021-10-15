
 
pub mod settings{
    use config::{Config};
    use tokio_postgres::Config as Config_Postgres;
    use std::sync::RwLock;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref SETTINGS: RwLock<Config> = {
        let mut settings: Config = Config::default();
            settings
            .merge(config::File::with_name("src/settings/settings.toml")).unwrap()
            .merge(config::Environment::with_prefix("APP")).unwrap();
            RwLock::new(settings)
        };
    }

   pub fn config() -> Result<String, Box<std::error::Error>>{
        let config = format!("host={host} user={user} port={port} password={password} dbname={dbname}",
        host=SETTINGS.read()?.get::<String>("host")?,
        user=SETTINGS.read()?.get::<String>("user")?,
        port=SETTINGS.read()?.get::<i32>("port")?,
        password=SETTINGS.read()?.get::<String>("password")?,
        dbname=SETTINGS.read()?.get::<String>("dbname")?);

        Ok(config)
   } 

   pub fn config_2() -> Result<Config_Postgres, Box<std::error::Error>>{
        let mut config:Config_Postgres = Config_Postgres::new();
        config.password(SETTINGS.read()?.get::<String>("password")?);
        config.user(&SETTINGS.read()?.get::<String>("user")?);
        config.port(SETTINGS.read()?.get::<u16>("port")?);
        config.dbname(&SETTINGS.read()?.get::<String>("dbname")?);
        config.host(&SETTINGS.read()?.get::<String>("host")?);

        Ok(config)
   }
}
