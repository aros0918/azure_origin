/*
    Azure implementation
*/

const config = require('../config');
const Protocol = require('azure-iot-device-mqtt').Mqtt;
// Uncomment one of these transports and then change it in fromConnectionString to test other transports
// var Protocol = require('azure-iot-device-amqp').AmqpWs;
// var Protocol = require('azure-iot-device-http').Http;
// var Protocol = require('azure-iot-device-amqp').Amqp;
// var Protocol = require('azure-iot-device-mqtt').MqttWs;

//const Client = require('azure-iot-device').ModuleClient;
const Client = require('azure-iot-device').Client;
const Message = require('azure-iot-device').Message;

var cloud = {};

cloud.init = function(onConnected, onMessage, onTwinUpdate){

    var client = Client.fromConnectionString(config.cloudConnectionString, Protocol);

    var receiveCount = 0;
    var sendCount = 0;
  
    var connectCallback = function (err) {
        if (err) {
            console.error('Could not connect: ' + err.message);
        } else {
            
            //console.log('Client connected');
            if (typeof onConnected === "function") {
                onConnected();
            }
            client.on('message', function (msg) {
                receiveCount++;
                console.log('Received Message '+ receiveCount +' Id: ' + msg.messageId, msg);
                if (typeof onConnected === "function") {
                    onConnected(msg);
                }
                // When using MQTT the following line is a no-op.
                client.complete(msg, printResultFor('completed'));
                // The AMQP and HTTP transports also have the notion of completing, rejecting or abandoning the message.
                // When completing a message, the service that sent the C2D message is notified that the message has been processed.
                // When rejecting a message, the service that sent the C2D message is notified that the message won't be processed by the device. the method to use is client.reject(msg, callback).
                // When abandoning the message, IoT Hub will immediately try to resend it. The method to use is client.abandon(msg, callback).
                // MQTT is simpler: it accepts the message by default, and doesn't support rejecting or abandoning a message.
            });

/*
            client.onMethod('doSomethingInteresting', function(request, response) {
                console.log('doSomethingInteresting called');
      
                if(request.payload) {
                  console.log('Payload:');
                  console.dir(request.payload);
                }
      
                var responseBody = {
                  message: 'doSomethingInteresting succeeded'
                };
                response.send(200, responseBody, function(err) {
                  if (err) {
                    console.log('failed sending method response: ' + err);
                  } else {
                    console.log('successfully sent method response');
                  }
                });
            });
*/
            
            client.on('error', function (err) {
                console.error(err.message);
            });

            client.on('disconnect', function () {
                client.removeAllListeners();
                client.open(connectCallback);
            });
        }
    
    };
    
    cloud.send = function(obj, callback) {
        var data = JSON.stringify(obj);
        var message = new Message(data);
        //message.properties.add('deviceId', config.deviceId);
        sendCount++;
        console.log('Sending message: ' +sendCount+ ' '+ message.getData());
        client.sendEvent(message, callback);
    };

    client.open(connectCallback);
	
    // Create device Twin
    client.getTwin(function(err, twin) {
      if (err) {
        console.error('could not get twin');
      } else {
        console.log('twin created');


        twin.on('properties.desired', function(delta) {
            console.log('new desired properties received:');
            console.log(JSON.stringify(delta));
			onTwinUpdate();
        });
	  }
	});
}

// Helper function to print results in the console
function printResultFor(op) {
    return function printResult(err, res) {
        if (err) console.log(op + ' error: ' + err.toString());
        if (res) console.log(op + ' status: ' + res.constructor.name);
    };
}

module.exports = cloud;