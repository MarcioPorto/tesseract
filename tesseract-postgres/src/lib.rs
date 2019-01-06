use failure::{Error, format_err};
use tesseract_core::{Backend, DataFrame};
extern crate futures;
extern crate futures_state_stream;
extern crate tokio_core;
extern crate tokio_postgres;

use futures::Future;
use futures_state_stream::StateStream;
use tokio_core::reactor::Core;
use tokio_postgres::{Connection, TlsMode};
// use futures::future::ok;
// use tokio::runtime::current_thread::Runtime;
// use tokio_core::reactor::Handle;

#[derive(Clone)]
pub struct Postgres {
    conn_url: String
}

impl Postgres {
    pub fn new(address: &str) -> Postgres {
        Postgres { conn_url: address.to_string() }
    }

    pub fn from_addr(address: &str) -> Result<Self, Error> {
        Ok(Postgres::new(address))
    }
}

impl Backend for Postgres {
    fn exec_sql(&self, _sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let mut core = Core::new().unwrap();
        let pg_tokio_fut = Connection::connect(self.conn_url.to_string(),
                                   TlsMode::None,
                                   &core.handle())
            .then(|c| {
                c.unwrap().prepare("SELECT * from test")
            })
            .and_then(|(s, c)| {
              c.query(&s, &[])
                .for_each(|_| {
                   println!("row detected");
                    // let row: i32 = rows.get(0);
                    // println!("Found row {:?}", row);
                })
            })
            .and_then(|c| {
                println!("GOT HERE!: {:?}", c);
                Ok(DataFrame::new())
            })
            .map_err(|e| {
                println!("ERROR HERE!");
                format_err!("{:?}", e)
            });

        
        // let z = core.run(pg_tokio_fut).unwrap();
        // let future = ok::<_, _>(DataFrame::new());
        // Box::new(future) // no error with this
        Box::new(pg_tokio_fut) // panic error with this
    }

    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::env;

    #[test]
    fn test_simple_query() {
        let postgres_db = env::var("POSTGRES_DATABASE_URL").unwrap();
        let sql = r"SELECT 1;";
        let postgres = Postgres::new(&postgres_db);
        let _r = postgres.exec_sql(sql.to_string()).wait().expect("fail1");
        // println!("{:?}", r);
    }
}
