mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn geometry_point() -> Result<(), Error> {
	let sql = "
		UPSERT city:london SET centre = (-0.118092, 51.509865);
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_polygon() -> Result<(), Error> {
	let sql = "
		UPSERT city:london SET area = {
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		};
		UPSERT city:london SET area = {
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386],
			]],
		};
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipoint() -> Result<(), Error> {
	let sql = "
		UPSERT city:london SET points = {
			type: 'MultiPoint',
			coordinates: [
				[-0.118092, 51.509865],
				[-0.118092, 51.509865]
			]
		};
		UPSERT city:london SET points = {
			type: 'MultiPoint',
			coordinates: [
				[-0.118092, 51.509865],
				[-0.118092, 51.509865],
			],
		};
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipolygon() -> Result<(), Error> {
	let sql = "
		UPSERT university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
				[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]]
			]
		};
		UPSERT university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
				[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]],
			],
		};
		SELECT * FROM university:oxford;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_inner_access() -> Result<(), Error> {
	let sql = "
		SELECT type, coordinates[0] as lng, coordinates[1] AS lat FROM type::point([-0.118092, 51.509865]);
		SELECT type, coordinates[0] as lng, coordinates[1] AS lat FROM (-0.118092, 51.509865);
		SELECT coordinates FROM {
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				]
			],
		};
		SELECT coordinates FROM {
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				],
				[
					[-0.38314819, 51.37692386], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				],
				[
					[110.38314819, 110.37692386], [110.38314819, 110.61460570],
					[110.38314819, 110.37692386],
				]
			],
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				coordinates: [
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							0.1785278,
							51.37692386
						],
						[
							0.1785278,
							51.6146057
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					]
				]
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				coordinates: [
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							0.1785278,
							51.37692386
						],
						[
							0.1785278,
							51.6146057
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					],
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					],
					[
						[
							110.38314819,
							110.37692386
						],
						[
							110.38314819,
							110.6146057
						],
						[
							110.38314819,
							110.37692386
						]
					]
				]
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn objects_coerce_into_geometries() -> Result<(), Error> {
	let queries = r#"
	LET $polygon = {
    type: "Polygon",
    coordinates: [[
        [78.8, 78.8]
    ]]
};
$polygon.is_geometry();
$polygon.is_object();

LET $chained = {
    type: "Polygon",
    coordinates: [[
        [78.8, 78.8]
    ]]
}.chain(|$o: geometry| {
    type: $o.type,
    coordinates: $o.coordinates
});

$chained.is_object();
$chained.is_geometry();
<geometry>$chained;
RETURN geo::is::valid($chained);"#;

	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(queries, &ses, None).await?;

	// Geometry is succesfully created
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	// It is a geometry
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::Bool(true));

	// It is not an object
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::Bool(false));

	// $chained parameter is created
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	// It is an object
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::Bool(true));

	// It is not a geometry
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::Bool(false));

	// Cast into geometry works
	let tmp = res.remove(0).result?;
	let geometry = Value::parse(r#"{ type: 'Polygon', coordinates: [[[78.8, 78.8]]] }"#);
	assert_eq!(tmp, geometry);

	// And geo function works with $chained as an input
	// because it is coerced
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::Bool(true));
	Ok(())
}
