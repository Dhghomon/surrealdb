mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::sql::Value;
use surrealdb_sql::dbs::Session;
use surrealdb_sql::err::Error;

#[tokio::test]
async fn model_count() -> Result<(), Error> {
	let sql = "
		CREATE |test:1000| SET time = time::now();
		SELECT count() FROM test GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[{
			count: 1000
		}]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn model_range() -> Result<(), Error> {
	let sql = "
		CREATE |test:101..1100| SET time = time::now();
		SELECT count() FROM test GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[{
			count: 1000
		}]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
