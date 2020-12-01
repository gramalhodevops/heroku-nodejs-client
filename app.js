//Required Packages
const express = require('express');
const path = require('path');
const Kafka = require('no-kafka');
var fs = require("fs");
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var bodyParser = require('body-parser');
var nforce = require('nforce');
const { Client } = require('pg');
var fs = require('fs');

/*///////////////////////////////
    Postgres DB Setup Begining
*////////////////////////////////

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});
    // Starting Postgres Client Connection
       client.connect();

/*///////////////////////////////
    Postgres DB Setup END
*////////////////////////////////
 
/*///////////////////////////////
    Salesforce Orgs Setup Begining
*////////////////////////////////
//
// Query on ConfigSFConnections Table
var Org = [];
var OpEvent = [];
client.query('SELECT * from public.\"ConfigSFConnections\" where \"Status\" = \'Ativo\' ', (err, res) => {
    if (err) throw err;
    //for (let row of res.rows) {
        const data = res.rows;
        data.forEach(row => {

        //Definin Org Details
        Org[row.Org]  = nforce.createConnection({
        clientId: row.clientId,
        clientSecret: row.clientSecret,
        redirectUri: row.redirectUri,
        mode: row.mode,// optional, 'single' or 'multi' user mode, multi default
        autoRefresh: true // <--- set this to true
        });

        console.log('Create Connection ' +[row.Org]+ ':' + JSON.stringify(Org[row.Org]));

        // Defining Endpoint
        OpEvent[row.Org]  = nforce.createSObject(row.EventObj);

        // Autenticating
        Org[row.Org].authenticate({ username: row.username, password: row.password}, function(err, resp){
            if (err) throw err;
            console.log('Connection Response' +[row.Org]+ ':' + resp);
            }); 
        })
        });

/*///////////////////////////////
    Salesforce Orgs Setup END
*////////////////////////////////

/*///////////////////////////////
    KAFKA Setup Begining
*////////////////////////////////

//var brokerUrls = 'kafka+ssl://ec2-107-21-185-96.compute-1.amazonaws.com:9096,kafka+ssl://ec2-54-196-166-80.compute-1.amazonaws.com:9096,kafka+ssl://ec2-3-234-20-129.compute-1.amazonaws.com:9096';
//var brokerUrls = brokerUrls.replace(/\+ssl/g,'');
var brokerUrls = process.env.KAFKA_URL.replace(/\+ssl/g,'');
var consumer = new Kafka.SimpleConsumer({
  connectionString: brokerUrls,
  ssl: {
    //cert: process.env.KAFKA_TRUSTED_CERT,//fs.readFileSync("/Users/gramalho/Downloads/certificate/kafka.crt").toString(),
    //key: process.env.KAFKA_CLIENT_CERT_KEY//fs.readFileSync("/Users/gramalho/Downloads/certificate/kafka.key").toString(),
    cert: process.env.KAFKA_CLIENT_CERT,
    key: process.env.KAFKA_CLIENT_CERT_KEY,
    // secureProtocol: 'TLSv1_method',
    rejectUnauthorized: false
  }
});
/*///////////////////////////////
    KAFKA Setup END
*////////////////////////////////


var dataHandler = function(messageSet, topic, partition) {
    messageSet.forEach(function(m) {
        
        var data = JSON.parse(m.message.value.toString('utf8'));

        //Query on ConfigData based on Products
        var obj = JSON.parse(m.message.value);
        var destORg = '';
        var varProds = obj.payload.after.products__c.replace(/;/g, ',');
        var inClause = '\'' + varProds.split(',').join('\',\'') + '\'';
        const res1 = client.query('SELECT * from public.\"ConfigData\" where \"Produto\" in (' +inClause+ ') and \"Status\" = \'Ativo\'', async (err, res) => {
        if (err) throw err;
        const rescdata = res.rows;

        for (var i = 0; i < rescdata.length; ++i) {

            if (rescdata[i].Schema != obj.payload.source.schema){
                try {
                        const dataRawPostgres = await client.query('SELECT * from '+  obj.payload.source.schema + '.' +  '\"' + obj.payload.source.table + '\" where \"sfid\" = \'' + obj.payload.after.sfid + '\'');
                        const dataPostgres = dataRawPostgres.rows;
  
                        dataPostgres.forEach(row => {
                                                        OpEvent[rescdata[i].Org].set('Amount__c', row.amount);
                                                        OpEvent[rescdata[i].Org].set('CloseDate__c', row.closedate);
                                                        OpEvent[rescdata[i].Org].set('Customer_Code__c', row.account_number__c);
                                                        OpEvent[rescdata[i].Org].set('Description__c', row.description);
                                                        OpEvent[rescdata[i].Org].set('Event_Type__c', obj.payload.op);
                                                        OpEvent[rescdata[i].Org].set('Name__c', row.name);
                                                        OpEvent[rescdata[i].Org].set('OriginEventOrg__c', obj.payload.source.schema);
                                                        OpEvent[rescdata[i].Org].set('Product_1__c', row.products__c);
                                                        OpEvent[rescdata[i].Org].set('Salesforce_Origin_Id__c', row.sfid); 
                                                        OpEvent[rescdata[i].Org].set('Stage__c', row.stagename);
                                                        OpEvent[rescdata[i].Org].set('Type__c', row.type);
                                                        OpEvent[rescdata[i].Org].set('Payload__c', JSON.stringify(obj.payload));
                                                        destORg = rescdata[i].Org;

                                                        console.log('<<<<<<<< ATTEMPT TO PUBLISH SALEFORCE EVENT >>>>>>> ' +i);
                                                        console.log('<<<<<<<<<<<<<<<< EVENT DATE: '+new Date());
                                                        console.log('<<<<<<<< Source ORG: ' + obj.payload.source.schema);
                                                        console.log('<<<<<<<< Destination ORG: ' + destORg);
                                                        console.log('<<<<<<<< Check Salesforce Org Call Response >>>>>>>');

                                                        Org[rescdata[i].Org].insert({ sobject: OpEvent[rescdata[i].Org], oauth: Org[rescdata[i].Org].oauth }, function(err, resp){
                                                                if (err) throw err; 
                                                                console.log('<<<<<<<< Salesforce Org Call Response : '+JSON.stringify(resp));

                                                        });

                                                     });
                    } catch (err) {
                        console.error(err);
                        throw err;
                    }
        }
        }
        });

        // KAFKA LOG EVENTS
        console.log('<<<<<<<<<<<<<<<< EVENT RECEIVED >>>>>>>>>>>>>>>>');
        console.log('<<<<<<<<<<<<<<<< SOURCE: '+obj.payload.source.schema);
        console.log('<<<<<<<<<<<<<<<< TX ID: '+obj.payload.source.txId);
        console.log('<<<<<<<<<<<<<<<< OPERATION: '+obj.payload.op);
        console.log('<<<<<<<<<<<<<<<< OFFSET: '+m.offset);new Date();
        console.log('<<<<<<<<<<<<<<<< EVENT DATE: '+new Date());
        console.log('<<<<<<<<<<<<<<<< PAYLOAD >>>>>>>>>>>>>>>>');
        console.log('AccountId: '+obj.payload.after.accountid);
        console.log('Opportunity Name: '+obj.payload.after.name);
        console.log('Revenue: '+obj.payload.after.expectedrevenue);
        console.log('Stage Name: '+obj.payload.after.stagename);
        console.log('Products: '+varProds);
        console.log('SF ID: '+obj.payload.after.sfid);
        console.log('<<<<<<<<<<<<<<<< Full Payload Kafka >>>>>>>>>>');
        console.log(obj.payload);
        console.log('<<<<<<<<<<<<<<<< END OF EVENT >>>>>>>>>>>>>>>>');
        var packet = {};
        packet.offset = m.offset;
        packet.messageSize = m.messageSize;
        packet.data = data;
        io.emit('message', JSON.stringify(packet));
    });
}

/*
    Subscribing to all kafka topics - Opportunitty/Lead/Chatter Feed
*/
// Query to identify the topics that should be subscribed
const kafkaQuery = client.query('SELECT * from public.\"ConfigData\" where \"Status\" = \'Ativo\'', async (err, res) => {
    if (err) throw err;
    const kafkaData = res.rows;
    var kafkaTopic = '';
    for (var i = 0; i < kafkaData.length; ++i) {

            kafkaTopic = kafkaData[i].Kafka_Topic;
            console.log('>>>>> Kafka topic : ' + kafkaTopic);
            const kafkaSub = await consumer.init().then(function() {
                return consumer.subscribe(kafkaTopic, dataHandler);    
            });
            }
        });

/*
    Webserver setup
*/
app.use(express.static('public'));
app.use(bodyParser.json());


