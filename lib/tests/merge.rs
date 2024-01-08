mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::sql::Value;
use surrealdb_sql::dbs::Session;
use surrealdb_sql::err::Error;

#[tokio::test]
async fn merge_record() -> Result<(), Error> {
	let sql = "
		UPDATE person:test SET name.initials = 'TMH', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
		UPDATE person:test MERGE {
			name: {
				title: 'Mr',
				initials: NONE,
				suffix: ['BSc', 'MSc'],
			}
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: {
					initials: 'TMH',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					suffix: ['BSc', 'MSc'],
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
