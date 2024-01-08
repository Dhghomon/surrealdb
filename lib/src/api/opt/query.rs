use crate::api::{err::Error, Response as QueryResponse, Result};
use crate::method::Stats;
use serde::de::DeserializeOwned;
use std::mem;
use surrealdb_sql::from_value;
use surrealdb_sql::syn;
use surrealdb_sql::Query;
use surrealdb_sql::{self, statements::*, Array, Object, Statement, Statements, Value};

/// A trait for converting inputs into SQL statements
pub trait IntoQuery {
	/// Converts an input into SQL statements
	fn into_query(self) -> Result<Vec<Statement>>;
}

impl IntoQuery for Query {
	fn into_query(self) -> Result<Vec<Statement>> {
		let Query(Statements(statements)) = self;
		Ok(statements)
	}
}

impl IntoQuery for Statements {
	fn into_query(self) -> Result<Vec<Statement>> {
		let Statements(statements) = self;
		Ok(statements)
	}
}

impl IntoQuery for Vec<Statement> {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(self)
	}
}

impl IntoQuery for Statement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![self])
	}
}

impl IntoQuery for UseStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Use(self)])
	}
}

impl IntoQuery for SetStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Set(self)])
	}
}

impl IntoQuery for InfoStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Info(self)])
	}
}

impl IntoQuery for LiveStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Live(self)])
	}
}

impl IntoQuery for KillStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Kill(self)])
	}
}

impl IntoQuery for BeginStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Begin(self)])
	}
}

impl IntoQuery for CancelStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Cancel(self)])
	}
}

impl IntoQuery for CommitStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Commit(self)])
	}
}

impl IntoQuery for OutputStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Output(self)])
	}
}

impl IntoQuery for IfelseStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Ifelse(self)])
	}
}

impl IntoQuery for SelectStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Select(self)])
	}
}

impl IntoQuery for CreateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Create(self)])
	}
}

impl IntoQuery for UpdateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Update(self)])
	}
}

impl IntoQuery for RelateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Relate(self)])
	}
}

impl IntoQuery for DeleteStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Delete(self)])
	}
}

impl IntoQuery for InsertStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Insert(self)])
	}
}

impl IntoQuery for DefineStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Define(self)])
	}
}

impl IntoQuery for RemoveStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Remove(self)])
	}
}

impl IntoQuery for OptionStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Option(self)])
	}
}

impl IntoQuery for &str {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(self)?.into_query()
	}
}

impl IntoQuery for &String {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(self)?.into_query()
	}
}

impl IntoQuery for String {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(&self)?.into_query()
	}
}

/// Represents a way to take a single query result from a list of responses
pub trait QueryResult<Response>
where
	Response: DeserializeOwned,
{
	/// Extracts and deserializes a query result from a query response
	fn query_result(self, response: &mut QueryResponse) -> Result<Response>;

	/// Extracts the statistics from a query response
	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(&0).map(|x| x.0)
	}
}

impl QueryResult<Value> for usize {
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Value> {
		match map.remove(&self) {
			Some((_, result)) => Ok(result?),
			None => Ok(Value::None),
		}
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Option<T>> {
		let value = match map.get_mut(&self) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&self);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let result = match value {
			Value::Array(Array(vec)) => match &mut vec[..] {
				[] => Ok(None),
				[value] => {
					let value = mem::take(value);
					from_value(value).map_err(Into::into)
				}
				_ => Err(Error::LossyTake(QueryResponse(mem::take(map))).into()),
			},
			_ => {
				let value = mem::take(value);
				from_value(value).map_err(Into::into)
			}
		};
		map.remove(&self);
		result
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(self).map(|x| x.0)
	}
}

impl QueryResult<Value> for (usize, &str) {
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Value> {
		let (index, key) = self;
		let response = match map.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(Value::None);
			}
		};

		let response = match response {
			Value::Object(Object(object)) => object.remove(key).unwrap_or_default(),
			_ => Value::None,
		};

		Ok(response)
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Option<T>> {
		let (index, key) = self;
		let value = match map.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let value = match value {
			Value::Array(Array(vec)) => match &mut vec[..] {
				[] => {
					map.remove(&index);
					return Ok(None);
				}
				[value] => value,
				_ => {
					return Err(Error::LossyTake(QueryResponse(mem::take(map))).into());
				}
			},
			value => value,
		};
		match value {
			Value::None | Value::Null => {
				map.remove(&index);
				Ok(None)
			}
			Value::Object(Object(object)) => {
				if object.is_empty() {
					map.remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				from_value(value).map_err(Into::into)
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Vec<T>> {
		let vec = match map.remove(&self) {
			Some((_, result)) => match result? {
				Value::Array(Array(vec)) => vec,
				vec => vec![vec],
			},
			None => {
				return Ok(vec![]);
			}
		};
		from_value(vec.into()).map_err(Into::into)
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Vec<T>> {
		let (index, key) = self;
		let mut response = match map.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => match val {
					Value::Array(Array(vec)) => mem::take(vec),
					val => {
						let val = mem::take(val);
						vec![val]
					}
				},
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(vec![]);
			}
		};
		let mut vec = Vec::with_capacity(response.len());
		for value in response.iter_mut() {
			if let Value::Object(Object(object)) = value {
				if let Some(value) = object.remove(key) {
					vec.push(value);
				}
			}
		}
		from_value(vec.into()).map_err(Into::into)
	}

	fn stats(&self, QueryResponse(map): &QueryResponse) -> Option<Stats> {
		map.get(&self.0).map(|x| x.0)
	}
}

impl QueryResult<Value> for &str {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Option<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Vec<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		(0, self).query_result(response)
	}
}
