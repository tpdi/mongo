/*
	Tests Aggregation Framework date expressions, both UTC and local.
	Self-contained, can be run independent of any of file or data.
*/

var testDates = function() {

	var isLeapYear = function(y) {
		return y % 400 == 0 || ( y % 4 == 0 && ! (y % 100 == 0 ));
	};

	var getDayOfYearYMD = function(y, m, d) {
		return m == 0 ? d 
			: m == 1 ? 31 + d 
			: Math.floor(30.6 * (m + 2)) + d - 122 + ( isLeapYear( y ) ? 60 : 59 ); 

	} ;

	var getDayOfYear = function(dd) {
		return getDayOfYearYMD( dd.getFullYear(), dd.getMonth(), dd.getDate());
	} ;

	var getUTCDayOfYear = function(dd) {
		return getDayOfYearYMD( dd.getUTCFullYear(), dd.getUTCMonth(), dd.getUTCDate());
	} ;

	var getWeekDYD = function( dayOfYear, day ) {
		return Math.floor( ( ( dayOfYear -  ( day + 1 ) + 7 ) / 7 )) ;
	};

	var getWeek = function (d) {
		return getWeekDYD( getDayOfYear(d), d.getDay() ) ;
	};

	var getUTCWeek = function (d) {
		return getWeekDYD( getUTCDayOfYear(d), d.getUTCDay() ) ;
	};

	var makeProjection = function( jsDateFuncToAggExp, UTCArgValue ) {
		var proj = { 'aDate': 1 };

		
		var prefixes = { 
			'' : true ,
			'UTC': UTCArgValue
		};
		
		for( var j in prefixes ) {
			for( var k in jsDateFuncToAggExp ) {
				var v = {};
				var arglist = [ '$aDate' ];
				if( prefixes[j] !== null ) {
					arglist.push( prefixes[j] ) ;
				}
				v[jsDateFuncToAggExp[k][0]] = arglist ;
				proj[ j + k ] =  v;
					
			}
		}
		
		return { '$project' : proj };
	};

	var nonMembers = { 
		getUTCWeek: getUTCWeek, 
		getWeek : getWeek,
		getUTCDayOfYear: getUTCDayOfYear,
		getDayOfYear: getDayOfYear
	};

	var jsDateFuncToAggExp = {
			 'Seconds'	:	[ '$second' , 0],
			 'Minutes'	:	[ '$minute' , 0],
			 'Hours'	:	[ '$hour' , 0],
			 'Week': 		[ '$week', 0],
			 'Month'	: 	[ '$month' , 1],		
			 'FullYear'	: 	[ '$year' , 0],
			 'Day'		: 	[ '$dayOfWeek' , 1],
			 'Date'		: 	[ '$dayOfMonth', 0],
			 'DayOfYear': 	[ '$dayOfYear', 0]
		};

		
	var testEach = function( i ) {
		var aDate = i.aDate;
		//print( tojson(aDate), ' ', aDate.toString() );
		//delete i.aDate;
		for( var k in i ) {
			
			var ks = k;
			if( k.startsWith('UTC') ){
				ks = ks.substr(3);
			}
			
			var jsf = 'get' + k;

			if( aDate[jsf]) {
				assert.eq( aDate[jsf]() + jsDateFuncToAggExp[ks][1], i[k], 
					jsDateFuncToAggExp[ks][0] + " is broken (for " + aDate + ")" );
					//print( tojson(aDate), ' ', aDate.toString(), k, ' ', aDate[jsf]() + jsDateFuncToAggExp[ks][1],  ' ', i[k] );
			} else if( nonMembers[jsf] ) {
				assert.eq( nonMembers[jsf](aDate) + jsDateFuncToAggExp[ks][1], i[k], 
					jsDateFuncToAggExp[ks][0] + " is broken (for " + aDate + ")" );
					//print( tojson(aDate), ' ', aDate.toString(), k, ' ', nonMembers[jsf](aDate) + jsDateFuncToAggExp[ks][1], ' ', i[k] );
			}

		}
	};

	var fillData = function( col) {
		var sec = 1000;
		var min = 60 * sec;
		var hour = 60 * min;
		var day = 24 * hour; 
		var insertedCount = 0;
		
		for( i = 1970; i < 2039; ++i ) {
			var aDate = new Date( i, 0, 1) ;
			col.insert( { aDate: aDate } );
			++insertedCount;
			var accum = 0 ;
			for( var j = 0 ; j < 12; ++j ) {
				accum += 29 * day + 3 * hour + 29 * min + 29 * sec ;
				var d = new Date( aDate.getTime() + accum );
				col.insert( { aDate: d } );
				++insertedCount;
			}
		}
		return insertedCount;
	}

	var runTest = function( UTCArgValue ) {
		var collection = db.testDateExpressions;
		try {
			collection.remove();
			var insertedCount = fillData( collection );
			//print( tojson( makeProjection( jsDateFuncToAggExp, UTCArgValue )));
			var r = collection.aggregate( makeProjection( jsDateFuncToAggExp, UTCArgValue ) );
			//print ( tojson(r) ) ;
			assert( r && r.result && r.result.length == insertedCount, r );
			r.result.forEach( testEach ) ;
		} finally {
			collection.remove();
		}
	};
	
	// run with UTC value expression passed explictly false, 
	// e.g, {$hour: ['$aDate', false]}
	runTest(false); 
	// run with UTC value expression absent and implicitly false, as before 
	// e.g, {$hour: ['$aDate']}
	runTest(null);
	print('Agregation date expression tests pass.');
}();