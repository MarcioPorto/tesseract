use actix_web::{
    http::Method,
    middleware,
    App,
};
use clickhouse_rs:: Options as ChOptions;

use crate::handlers::{
    index_handler,
    test_handler,
};

pub struct AppState {
    pub clickhouse_options: ChOptions,
}

pub fn create_app(clickhouse_options: ChOptions) -> App<AppState> {
    App::with_state(AppState { clickhouse_options: clickhouse_options })
        .middleware(middleware::Logger::default())
        .resource("/", |r| {
            r.method(Method::GET).with(index_handler)
        })
        .resource("/test/{schema}/{table}", |r| {
            r.method(Method::GET).with(test_handler)
        })
}