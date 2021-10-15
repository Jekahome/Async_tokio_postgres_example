## Basic example using async tokio-postgres

```shell
Install docker postgres
$ sudo docker pull postgres

$ docker run --name rust_job_queue  -e POSTGRES_USER=rust -e POSTGRES_PASSWORD=job_queue -p 5432:5432 postgres:latest
```

```shell
# restart docker job
$ docker start rust_job_queue

# remove
$ docker rm -f rust_job_queue
```

## Links
[Example tokio-postgres test](https://github.com/sfackler/rust-postgres/tree/master/tokio-postgres/tests/test)

[Types INT4,VARCHAR ...](https://github.com/sfackler/rust-postgres/blob/d45461614aca87022c17a2cc26b22325bf161fa5/postgres-types/src/lib.rs#L375)

[Client tokio-postgres documentation](https://docs.rs/postgres/0.19.0/postgres/struct.Client.html#method.query_one)

[For type safety impl traite ToSql FromSql](https://github.com/sfackler/rust-postgres/blob/fc10985f9fdf0903893109bc951fb5891539bf97/postgres-protocol/src/types/mod.rs#L97)

[PostgresQL documentation](https://www.postgresql.org/docs/9.0/sql-listen.html)
