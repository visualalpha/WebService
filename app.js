const async = require('async');
const { spawn, execSync, spawnSync } = require("child_process");
const AWS = require('aws-sdk');
const handlebars = require('handlebars');
var superagent = require('superagent');
const fs = require('fs').promises;

let RUNTIME_ENV = "dev";

let config = {
	"STATIC_CONTENT_PATH": {"dev": "./", "test": "/opt/", "qa": "/opt/"},
	"DEFAULT_AWS_CONFIG_PATH": "",
	"DEFAULT_UPDATE_DB_FETCH_RANGE": 60 * 5,
	"DEFAULT_CHART_LENGTH": 3600 * 24 * 1000,
	"DEFAULT_CHART_UPDATE_LENGTH": 600 * 1000,
	"BINANCE_FETCH_SIZE": 3600 * 6 * 1000,
	"BINANCE_PRED_FETCH_SIZE": 1800 * 1000
}

module.exports.dashboard = async (event, context) => {

    // we check for AWS_LAMBDA_FUNCTION_NAME env var to see if we are running inside lambda or somewhere else
    if ('RUNTIME_ENV' in process.env)
    	RUNTIME_ENV = process.env.RUNTIME_ENV;

    // if local dev then we need to explicitly load the default aws config file with regions, etc
    if(RUNTIME_ENV == "dev")
    	AWS.config.update({region:'ap-northeast-1'});

    let cmd = event.queryStringParameters.cmd;
	let func = null;
	let router = new Router();
    switch ( cmd ) {
    	case 'get_page':
    		func = router.get_page;
    		break;
    	case 'get_whale_buckets':
    		func = router.get_whale_buckets;
    		break;
    	case 'update_db':
    		func = router.update_db;
    	case 'get_config':
    		func = router.get_config;
    		break;
    	case 'generate_dataset':
    		func = router.generate_dataset;
    	case 'get_price_pred':
    		func = router.price_pred;
	}
  
	try {
		let res = await func.bind(router)(event.queryStringParameters);
  		return res;
	} catch (err) {
		console.log(err);
    	return err;
	}
}

class Router
{
	constructor()
	{
		this.alert_api = new WhaleAlertApi();
		this.db_handle = new DBStore();
		this.binance = new BinanceApi();
		this.ds = new Dataset( this.binance );
		this.predictor = new PricePredictor( this.binance, this.ds );
	}

	respond( {body, type = "html", status = 200} )
	{
		let type_header = 'text/html; charset=utf-8';
		if( type == "json" )
			type_header = 'application/json';
		var response = {
			statusCode: status,
			headers: { 'Content-Type': type_header },
			body: body
		}
		return response;
	}

	async price_pred()
	{
		let res = await this.predictor.pred();
		return this.respond( {body:JSON.stringify( res ), type:"text" } );
	}

	async generate_dataset()
	{
		let res = await this.binance.dataset_generator();
		return this.respond( {body:JSON.stringify( res ), type:"text" } );
	}

	async get_page( params )
	{
		let source_path = config.STATIC_CONTENT_PATH[RUNTIME_ENV] + 'static/main.handlebars';
		let source = await fs.readFile( source_path, "utf8");
		return this.respond( {body:source } );;
	}

	// second end-point to fetch bucket data
	async get_whale_buckets( params )
	{
		console.log( params );
	    let api_data = await this.db_handle.get_from_db( params );
		let buckets =  this.alert_api.bucket_data_exchange( api_data );
		if( !buckets ) return { statusCode: 500, body: 'Something wrong getting data buckets!' };
		return this.respond( {body:JSON.stringify( buckets ), type:"json" } );
	}

	async update_db( params )
	{
		console.log( params );
	    let api_data = await this.alert_api.get_data( params );
	    
	    if( !api_data ) return { statusCode: 500, body: 'Error fetching data from downstream API!' };

	    let save_res = await this.db_handle.save_to_db( api_data );
	    
	    let count = api_data.transactions.length;
	    var response = this.respond( {body:'{"msg":"all '+count+' records saved to db"}', type:"json" } );
		return response;
	}
}

class Dataset
{
	constructor( web_api ) { this.web_api = web_api; }

	async dataset_generator()
	 {
	 	// 1 day => 1440 frames
	 	let days = 1; // about 1sec per day? //37~49sec for 50days // 90sec for 100days
	 	let winsize = 0; // 27 * (winsize + 1) 30 => 837 features
        let whitelist = ["open", "close", "macd1_hist", "macd5_hist"];
        let end_dateiso = "2021-01-17";
        
	 	for( days of [1, 2, 10, 20] ){
	 		for( winsize of [0, 1, 2, 4, 10] ){
	 			console.log(days, winsize );
	 			await this._dataset_generator_impl( days, winsize, end_dateiso, whitelist );
	 		}
	 	}
    return "all good!";
	 }

    async save_csv( filepath1, data1, header1, filepath2, data2, header2, filepath3, data3, header3 )
    {
    	await fs.writeFile(filepath1, "", 'utf8');

		for( const line of data1 )
			await fs.appendFile(filepath1, line+`\n`, 'utf8');
    	
    	await fs.writeFile(filepath2, "", 'utf8');
		await fs.appendFile(filepath3, data3, 'utf8');
    	for( const line of data2 )
			await fs.appendFile(filepath2, line+`\n`, 'utf8');
    }

	async _dataset_generator_impl( days, winsize, end_dateiso, whitelist = null )
	{
	 	let end_date = new Date(end_dateiso);
        let start_date = new Date(end_date);
        start_date.setDate( end_date.getDate() - days + 1 );

        let postfix = "win"+winsize.toString().padStart(2, "0") +"_"+ days.toString().padStart(3, "0");
        let csv_filepath = './datasets/data_binance_'+ this.date2datestamp( end_date ) +"_"+postfix+'_data.csv';
        let label_filepath = './datasets/data_binance_'+ this.date2datestamp( end_date ) +"_"+postfix+'_labels.csv';
        let sample_filepath = './datasets/data_binance_'+ this.date2datestamp( end_date ) +"_"+postfix+'_samples.csv';

        let data = await load_data( start_date.toISOString(), end_date.toISOString() );
        let data_obj = await this.json2csv( data, winsize, whitelist );

        this.save_csv( csv_filepath, data_obj["csv_data"], data_obj["csv_header"], label_filepath, 
        				data_obj["label_data"], data_obj["label_header"], sample_filepath, data_obj["sample_data"], data_obj["sample_header"] );
	}

	date2datestamp( date_obj )
	{
		return (date_obj.getYear()-100).toString() + (date_obj.getMonth()+1).toString() +
					(date_obj.getDate()).toString().padStart(2, "0");
	}

    prefix_comma_to_line( line )
    {
    	return ", " + line;
    }

    async json2csv( json_obj, winsize, feature_whitelist = null, is_sample_mode = false )
    {
    	let header = new DatasetHeader( winsize, feature_whitelist );

    	let label_header = "next_close_val, next_close_dir, next_close_bps";
    	label_header += ", next_close_val5, next_close_dir5, next_close_bps5, next_close_val30, next_close_dir30, next_close_bps30, next_close_val60, next_close_dir60, next_close_bps60, next_close_val120, next_close_dir120, next_close_bps120";

    	// extract close price into an array so we can calculate ema on it
    	let close_vec = [];
    	for( let entry of json_obj )
    		close_vec.push( {x:entry[0], y:entry[4]} );
    	let macd = this.calculate_macd( close_vec, [ 1, 5, 30 ] );

    	let csv_data = [];
    	let label_data = [];
    	let sample_data = null;
    	let sample_count = json_obj.length * 0.1; // we assume last 10% of data will be used for samples
    	let loop_max = json_obj.length-120;

    	for( let i = winsize; i < loop_max; i++)
    	{
    		let entry = json_obj[i];
    		let line = this.extract_line( header, json_obj, i );
    		line += this.prefix_comma_to_line( this.extract_bps( header, json_obj, i ));
    		line += this.prefix_comma_to_line( this.extract_macd( header, macd, i ));

    		for(let q = -1; q >= -winsize; q--)
	    	{
				line += this.prefix_comma_to_line( this.extract_line( header, json_obj, i+q ));
	    		line += this.prefix_comma_to_line( this.extract_bps( header, json_obj, i+q ));
	    		line += this.prefix_comma_to_line( this.extract_macd( header, macd, i+q ));
	    	}

	    	if( i > (json_obj.length - sample_count) )
	    		sample_data.push( line );
	    	else
	    		csv_data.push( line );

    		var label_line = "";
    		label_line = this.calculate_label( close_vec[i+1].y, entry );
    		label_line += this.prefix_comma_to_line( this.calculate_label( close_vec[i+5].y, entry ));
    		label_line += this.prefix_comma_to_line( this.calculate_label( close_vec[i+30].y, entry ));
    		label_line += this.prefix_comma_to_line( this.calculate_label( close_vec[i+60].y, entry ));
    		label_line += this.prefix_comma_to_line( this.calculate_label( close_vec[i+120].y, entry ));
	    	label_data.push( label_line );
    	}
    	
    	return { "csv_header":header, "csv_data":csv_data,
    			"label_header":label_header, "label_data":label_data,
    			"sample_header":"", "sample_data":sample_data };
    }

    extract_line( header, json_obj, index )
    {
    	let entry = json_obj[index];
    	let line = "";
    	let is_first = true;
    	for( let feature in header.raw_features_indexes )
    		if( header.is_feature_in_header( feature ) )
    		{
    			if( !is_first )
    				line += ", ";
    			line += `${entry[ header.raw_features_indexes[feature] ]}`
    			is_first = false;
    		}
    	return line;
    }

    extract_bps( header, json_obj, index )
    {
    	let entry = json_obj[index];
    	let open =  entry[1];
		let high_bps = this.num2bps( entry[2], open );
		let low_bps = this.num2bps( entry[3], open );
		let close_bps = this.num2bps( entry[4], open );
		let line = "";
		if( header.is_feature_in_header( "high_bps" ) )
			line += high_bps;
		if( header.is_feature_in_header( "low_bps" ) )
			line += low_bps;
		if( header.is_feature_in_header( "close_bps" ) )
			line += close_bps;
		return line;
    }

    extract_macd( header, macd_obj, index )
    {
    	let macd = macd_obj;
    	let i = index;

    	let line = "";
    	let is_first = true;
    	for( let macd_prefix of ["1", "5", "30"] )
    	for( let feature of ["ema12", "ema26", "macd", "signal", "hist"] )
    		if( header.is_feature_in_header( "macd" + macd_prefix + "_" + feature ) )
    		{
    			if( !is_first )
    				line += ", ";
    			line += `${macd[ macd_prefix ][ feature ][ i ].y }`
    			is_first = false;
    		}
		return line;
    }

    calculate_label( future_close, entry )
    {
		var next_close_dir = future_close < entry[4] ? -1 : 1;
		var next_close_bps = this.num2bps( future_close, entry[1] );
		var label_line = `${future_close}, ${next_close_dir}, ${next_close_bps}`;
		return label_line;
    }

    num2bps( val, base) { return ( (100 / base * val) - 100 ) * 100; }

    // Moving Average Convergence/Divergence
    calculate_macd( dps, counts )
    {
    	let macd_list = {};
    	for( let base_count of counts)
    	{
    		let ema12 = calculateEMA( dps, base_count * 12 );
    		let ema26 = calculateEMA( dps, base_count * 26 );
    		let macd = [], hist =[];
			for(var i = 0; i < ema12.length; i++)
				macd.push({x: ema12[i].x, y: (ema12[i].y - ema26[i].y)});
			var ema9 = calculateEMA(macd, base_count * 9);
			for(var i = 0; i < ema12.length; i++)
			{
				let score = (macd[i].y - ema9[i].y);
				hist.push({x: ema12[i].x, y: score });
  			}
  			macd_list[base_count] = {"ema12":ema12, "ema26":ema26, "macd":macd, "signal":ema9, "hist":hist };
    	}
    	return macd_list;
    }
}


class PricePredictor
{
	constructor( web_api, dataset )
	{
		this.web_api = web_api;
		this.dataset = dataset;
	}

	async pred()
	{
		// get last ~20 frames from Binance
		let prices = await this.web_api.get_latest();
		let winsize = 10;
		let whitelist = ["open", "close", "vol", "num_of_trades", "macd1_hist", "macd1_macd", "macd1_signal", "macd5_hist", "macd5_macd", "macd5_signal", "macd30_ema12", "macd30_ema26", "macd30_macd", "macd30_signal", "macd30_hist"];

		// format it using dataset class calls to get a single sample point
		let csv_obj = await this.dataset.json2csv( prices, winsize, whitelist, true )

		let mode = "pred";
		let model = "model_210205.xgboost";
		let ppv = 0.636;
		let threshold = 0.6;
		let metric = "bps60+10";
		const pred = spawnSync("./PriceRunner", [ mode, model, csv_obj["sample_data"].slice(-1) ]);
		let output = String(pred.stdout);
		let score = output.split("\n")[3].slice(5,-1)
		return { "model":model, "ppv":ppv, "pred":score };
	}
}

class BinanceApi
{
	constructor(){}

	async get_latest()
	{
		let api_url = "https://fapi.binance.com/fapi/v1/continuousKlines";
		let start_timestamp = Date.now() - config.BINANCE_PRED_FETCH_SIZE;
		let end_timestamp = start_timestamp + config.BINANCE_PRED_FETCH_SIZE;
		let params = {"pair":"btcusdt", "interval":"1m", "contractType":"PERPETUAL", 
		          "startTime": start_timestamp, "endTime": end_timestamp };
		let prices = await this.fetch_url( api_url, params )
	    return prices;
	}

	async get_data( start_timestamp, end_timestamp )
    {
        let api_url = "https://fapi.binance.com/fapi/v1/continuousKlines";
        let prices = [];
        
        // the API gets only ~8h of data per call, so we loop over
        while( start_timestamp < end_timestamp )
        {
          let chunk_end_timestamp = start_timestamp + config.BINANCE_FETCH_SIZE;
          let params = {"pair":"btcusdt", "interval":"1m", "contractType":"PERPETUAL", 
                      "startTime": start_timestamp, "endTime": chunk_end_timestamp };
          let new_prices = await this.fetch_url( api_url, params )
          
          prices = [...prices, ...new_prices ];
          start_timestamp += config.BINANCE_FETCH_SIZE;
        }
        return prices;
    }

    async fetch_url( url, query = null ) {
    	return await superagent.get( url )
        .query( query )
        .then( data => { return (data.body); } )
        .catch( (err) => {
        	console.log("Too many requests to the API?");
        	console.log(err);
        	resolve(false);
        });
        console.log("returning true from get_data");
    }
}

class DatasetHeader
{
	constructor( winsize = 0, whitelist = null )
	{
		// 27 features
		this.features = { "raw":[], "bps":[], "macd1":[], "macd5":[], "macd30":[] };
		this.features.raw = ["open", "high", "low", "close", "vol", "quote_asset_vol", "num_of_trades", "taker_vol", "taker_buy_asset_vol"];
    	this.features.bps = ["high_bps", "low_bps", "close_bps"]; // no open_bps because it will always be 0 dy definition
    	this.features.macd1 = ["macd1_ema12", "macd1_ema26", "macd1_macd", "macd1_signal", "macd1_hist"];
    	this.features.macd5 = ["macd5_ema12", "macd5_ema26", "macd5_macd", "macd5_signal", "macd5_hist"];
    	this.features.macd30 = ["macd30_ema12", "macd30_ema26", "macd30_macd", "macd30_signal", "macd30_hist"];

    	this.raw_features_indexes = {
    		"open":1,
    		"high":2,
    		"low":3,
    		"close":4,
    		"vol":5,
    		"quote_asset_vol":7,
    		"num_of_trades":8,
    		"taker_vol":9,
    		"taker_buy_asset_vol":10
    	};

    	this.winsize = winsize;
    	this.whitelist = whitelist
	}

	is_feature_in_header( feature_name ) // asume feature name is valid
	{
		if( !this.whitelist )
			return true;
		if( this.whitelist && this.whitelist.indexOf( feature_name ) >= 0 )
			return true;
		return false;
	}
}

class DBStore {

	constructor()
	{
		this.db_handle = new AWS.DynamoDB.DocumentClient();
	}

    async save_to_db( data )
    {
		let total = data.transactions.length;

		for( let i = 0; i < total; i++)
    	{
    		let item = data.transactions[i]; 
    		let params = { TableName:"wtb_api_events-"+RUNTIME_ENV, Item: item };
				await this._save_data( item )
				.catch((error) => {
				    console.log(error);
				   //don't care about this error, just continue
				});
    	}
    	return {"body": "all good", statusCode: 200};
    }

	async _save_data( item ) {
	  let params = { Item: item, TableName:"wtb_api_events-"+RUNTIME_ENV, };
	  return this.db_handle.put(params).promise();
	}

	async get_from_db( {start_timestamp, end_timestamp} )
	{
		let params = {
			TableName:"wtb_api_events-test",
    		IndexName: "gsi-symbol",
    		ExpressionAttributeNames: { '#timestamp': 'timestamp' },
			ExpressionAttributeValues: { ':start_timestamp': Number(start_timestamp),
											':end_timestamp': Number(end_timestamp), ':symbol': "btc" },
			KeyConditionExpression: 'symbol = :symbol',
			FilterExpression: '#timestamp >= :start_timestamp AND #timestamp <= :end_timestamp'
		};

		let data = await this.db_handle.query( params ).promise();
		return data;
	}
}

module.exports.BinanceApi = BinanceApi;
module.exports.PricePredictor = PricePredictor;
module.exports.Dataset = Dataset;