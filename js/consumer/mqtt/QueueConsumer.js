/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Solace Web Messaging API for JavaScript
 * Persistence with Queues tutorial - Queue Consumer
 * Demonstrates receiving persistent messages from a queue
 */

/*jslint es6 browser devel:true*/
/*global solace*/

var QueueConsumer = function (queueName) {
    'use strict';
    var consumer = {};
    consumer.session = null;
    consumer.flow = null;
    consumer.queueName = queueName;
    consumer.consuming = false;

    // Logger
    consumer.log = function (line) {
        var now = new Date();
        var time = [('0' + now.getHours()).slice(-2), ('0' + now.getMinutes()).slice(-2),
        ('0' + now.getSeconds()).slice(-2)];
        var timestamp = '[' + time.join(':') + '] ';
        console.log(timestamp + line);
        var logTextArea = document.getElementById('log');
        logTextArea.value += timestamp + line + '\n';
        logTextArea.scrollTop = logTextArea.scrollHeight;
    };

    consumer.log('\n*** Consumer to queue "' + consumer.queueName + '" is ready to connect ***');

    // Establishes connection to Solace message router
    consumer.connect = function () {
        if (consumer.session !== null) {
            consumer.log('Already connected and ready to consume messages.');
            return;
        }
        var host = document.getElementById('host').value;
        // var port = 5050 // tcp
        var port = 8060; // ws

        var username = document.getElementById('username').value;
        var pass = document.getElementById('password').value;
        var vpn = document.getElementById('message-vpn').value;
        if (!host || !username || !pass || !vpn) {
            consumer.log('Cannot connect: please specify all the Solace message router properties.');
            return;
        }
        consumer.log('Connecting to Solace message router using url: ' + host);
        consumer.log('Client username: ' + username);
        consumer.log('Solace message router VPN name: ' + vpn);
        // create session
        try {
            consumer.session = new Paho.MQTT.Client(host, Number(port), '/' + vpn, "clientId");

            consumer.session.onConnectionLost = (responseObject) => {
                if (responseObject.errorCode !== 0) {
                    consumer.log("onConnectionLost:" + responseObject.errorMessage);
                }
            };
            consumer.session.onMessageArrived = (message) => {
                consumer.log('Received message: ' + message.payloadString.substr(0, 10));
                // console.log(message);
                // consumer.log('Received message: "' + message.payloadString + '"');
            };

            console.log('connecting as: ' + username + ', passwd: ' + pass);
            consumer.session.connect({
                'userName': '' + username,
                'password': '' + pass,
                'onSuccess': () => {
                    consumer.log('=== Successfully connected and ready to start the message consumer. ===');

                    consumer.session.subscribe(consumer.queueName, {
                        'onSuccess': () => {
                            consumer.log('=== Successfully subscribed to ' + consumer.queueName + '. ===');
                        },
                        'onFailure': (invocationContext, errorCode, errorMessage) => {
                            console.error(invocationContext, errorMessage);
                        }
                    });
                },
                'onFailure': (invocationContext, errorCode, errorMessage) => {
                    console.error(invocationContext, errorMessage);
                }
            });

        } catch (error) {
            console.error(error);
            consumer.log(error.toString());
        }
    };


    // Gracefully disconnects from Solace message router
    consumer.disconnect = function () {
        consumer.log('Disconnecting from Solace message router...');
        if (consumer.session !== null) {
            try {
                consumer.session.disconnect();
            } catch (error) {
                console.error(error);
                consumer.log(error.toString());
            }
        } else {
            consumer.log('Not connected to Solace message router.');
        }
    };

    return consumer;
};
