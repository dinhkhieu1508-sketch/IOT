// server.js
require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const mqtt = require('mqtt');

const app = express();

// ================= CONFIGURATION =================
const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI;
const MQTT_HOST = process.env.MQTT_HOST || "cdfa120916b24bceb90de3c3bb459f10.s1.eu.hivemq.cloud";
const MQTT_PORT = process.env.MQTT_PORT || 8883;
const MQTT_USER = process.env.MQTT_USER || "esp32";
const MQTT_PASS = process.env.MQTT_PASS || "Utc123456789";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "environment/sensor_data"; // ĐÚNG TOPIC CỦA BẠN

console.log('📋 Configuration:');
console.log(`  PORT: ${PORT}`);
console.log(`  MQTT Host: ${MQTT_HOST}`);
console.log(`  MQTT Port: ${MQTT_PORT}`);
console.log(`  MQTT Topic: ${MQTT_TOPIC}`);
console.log(`  MongoDB URI: ${MONGODB_URI ? '✅ Set' : '❌ Not set'}`);

// ================= MIDDLEWARE =================
app.use(cors());
app.use(express.json());

// ================= MONGO DB CONNECTION =================
async function connectToMongoDB() {
  try {
    if (!MONGODB_URI) {
      console.warn('⚠️ MongoDB URI not configured');
      return;
    }
    
    await mongoose.connect(MONGODB_URI);
    console.log('✅ Connected to MongoDB Atlas');
    console.log('📊 Database: iot_db');
    
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    console.log('⚠️  Continuing without MongoDB...');
  }
}

connectToMongoDB();

// ================= MONGOOSE SCHEMA =================
const SensorSchema = new mongoose.Schema({
  temperature: { type: Number },
  humidity: { type: Number },
  pm1_0: { type: Number },
  pm2_5: { type: Number },
  pm10_0: { type: Number },
  CO2: { type: Number },
  timestamp: { type: Date, default: Date.now }
});

const SensorData = mongoose.model('SensorData', SensorSchema);

// ================= SSE SETUP =================
let sseClients = [];

app.get('/sse', (req, res) => {
  console.log('🟢 New SSE client connected');
  
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });

  // Client ID
  const clientId = Date.now();
  
  // Send welcome message
  res.write(`data: ${JSON.stringify({
    type: 'CONNECTED',
    message: 'SSE Connected',
    clientId: clientId,
    timestamp: new Date().toISOString()
  })}\n\n`);

  // Store client
  sseClients.push({
    id: clientId,
    res: res,
    ip: req.ip
  });

  console.log(`📊 Active SSE clients: ${sseClients.length}`);

  // Send latest data if available
  sendLatestDataToClient(res);

  // Keep-alive every 25 seconds
  const keepAliveInterval = setInterval(() => {
    if (!res.finished) {
      res.write(`:keepalive\n\n`);
    }
  }, 25000);

  // Handle disconnect
  req.on('close', () => {
    console.log(`🔴 SSE client disconnected: ${clientId}`);
    clearInterval(keepAliveInterval);
    sseClients = sseClients.filter(client => client.id !== clientId);
    console.log(`📊 Remaining SSE clients: ${sseClients.length}`);
  });
});

// Send latest data to a specific client
async function sendLatestDataToClient(res) {
  try {
    if (mongoose.connection.readyState === 1) {
      const latestData = await SensorData.findOne().sort({ timestamp: -1 });
      if (latestData) {
        res.write(`data: ${JSON.stringify({
          type: 'SENSOR_DATA',
          data: latestData,
          timestamp: new Date().toISOString()
        })}\n\n`);
      }
    }
  } catch (error) {
    console.error('Error sending latest data:', error);
  }
}

// Broadcast to all clients
function broadcastSensorData(data) {
  const message = {
    type: 'SENSOR_DATA',
    data: data,
    timestamp: new Date().toISOString()
  };
  
  const sseMessage = `data: ${JSON.stringify(message)}\n\n`;
  
  sseClients.forEach((client, index) => {
    try {
      if (!client.res.finished) {
        client.res.write(sseMessage);
      }
    } catch (err) {
      console.error(`Error writing to client ${client.id}:`, err);
      sseClients.splice(index, 1);
    }
  });
}

// ================= MQTT CONNECTION =================
console.log(`🔗 Connecting to MQTT: ${MQTT_HOST}:${MQTT_PORT}`);

const mqttOptions = {
  host: MQTT_HOST,
  port: MQTT_PORT,
  protocol: 'mqtts',
  username: MQTT_USER,
  password: MQTT_PASS,
  clientId: 'nodejs-backend-' + Math.random().toString(16).substr(2, 8),
  clean: true,
  reconnectPeriod: 5000,
  connectTimeout: 30000
};

const mqttClient = mqtt.connect(mqttOptions);

mqttClient.on('connect', () => {
  console.log('✅ Connected to HiveMQ Cloud');
  console.log(`📡 Subscribing to topic: ${MQTT_TOPIC}`);
  
  mqttClient.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
    if (err) {
      console.error('❌ MQTT subscription error:', err);
    } else {
      console.log('✅ Successfully subscribed');
    }
  });
});

mqttClient.on('message', async (topic, message) => {
  try {
    console.log(`📩 MQTT received [${topic}]: ${message.toString()}`);
    
    const rawData = JSON.parse(message.toString());
    
    // Debug: log raw data structure
    console.log('📊 Raw data keys:', Object.keys(rawData));
    console.log('📊 Raw data values:', rawData);
    
    // Format sensor data - ĐÚNG VỚI FORMAT ESP32
    const sensorData = {
      temperature: Number(rawData.temperature) || 0,
      humidity: Number(rawData.humidity) || 0,
      pm1_0: Number(rawData.pm1_0) || 0,
      pm2_5: Number(rawData.pm2_5) || 0,
      pm10_0: Number(rawData.pm10_0) || 0,
      CO2: Number(rawData.CO2) || 0,
      timestamp: new Date()
    };

    
    console.log('📊 Parsed sensor data:', sensorData);

    // Save to MongoDB if connected
    if (mongoose.connection.readyState === 1) {
      try {
        const savedData = new SensorData(sensorData);
        await savedData.save();
        console.log('💾 Saved to MongoDB:', savedData._id);
        
        // Broadcast via SSE
        broadcastSensorData(savedData);
        
      } catch (dbError) {
        console.error('❌ MongoDB save error:', dbError.message);
        // Still broadcast even if DB save fails
        broadcastSensorData(sensorData);
      }
    } else {
      console.log('⚠️ MongoDB not connected, broadcasting only');
      broadcastSensorData(sensorData);
    }
    
  } catch (error) {
    console.error('❌ Error processing MQTT message:', error.message);
    console.error('Raw message was:', message.toString());
  }
});

mqttClient.on('error', (error) => {
  console.error('❌ MQTT error:', error.message);
});

// ================= REST API ENDPOINTS =================

// Get latest sensor data
app.get('/api/sensor-data/latest', async (req, res) => {
  try {
    if (mongoose.connection.readyState !== 1) {
      return res.json({
        temperature: 25.5,
        humidity: 65.0,
        pm1_0: 15,
        pm2_5: 35,
        pm10_0: 50,
        CO2: 800,
        timestamp: new Date(),
        message: 'Test data (MongoDB not connected)'
      });
    }
    
    const data = await SensorData.findOne().sort({ timestamp: -1 });
    
    if (!data) {
      return res.json({
        temperature: 25.5,
        humidity: 65.0,
        pm1_0: 15,
        pm2_5: 35,
        pm10_0: 50,
        CO2: 800,
        timestamp: new Date(),
        message: 'Test data (no records yet)'
      });
    }
    
    res.json(data);
    
  } catch (error) {
    res.status(500).json({
      error: 'Server error',
      message: error.message
    });
  }
});

// Get history
app.get('/api/sensor-data/history', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    
    if (mongoose.connection.readyState !== 1) {
      // Return test data if MongoDB not connected
      const testData = Array.from({ length: Math.min(limit, 10) }, (_, i) => ({
        temperature: 25.5 + (Math.random() * 5 - 2.5),
        humidity: 65.0 + (Math.random() * 20 - 10),
        pm1_0: Math.floor(15 + Math.random() * 20),
        pm2_5: Math.floor(35 + Math.random() * 30),
        pm10_0: Math.floor(50 + Math.random() * 40),
        CO2: Math.floor(800 + Math.random() * 300),
        timestamp: new Date(Date.now() - i * 600000)
      }));
      return res.json(testData);
    }
    
    const data = await SensorData.find()
      .sort({ timestamp: -1 })
      .limit(limit);
    
    res.json(data);
    
  } catch (error) {
    res.status(500).json({
      error: 'Server error',
      message: error.message
    });
  }
});

// Server status
app.get('/api/status', (req, res) => {
  res.json({
    server: 'running',
    timestamp: new Date().toISOString(),
    mongodb: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    mqtt: mqttClient.connected ? 'connected' : 'disconnected',
    sse_clients: sseClients.length,
    config: {
      mqtt_host: MQTT_HOST,
      mqtt_topic: MQTT_TOPIC,
      mongodb_connected: mongoose.connection.readyState === 1
    }
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK',
    timestamp: new Date().toISOString()
  });
});

// Test endpoint - simulate ESP32 data
app.post('/api/test/publish', (req, res) => {
  try {
    const testData = req.body || {
      temp: 25.5 + (Math.random() * 5 - 2.5),
      hum: 65.0 + (Math.random() * 20 - 10),
      pm1: Math.floor(15 + Math.random() * 20),
      pm25: Math.floor(35 + Math.random() * 30),
      pm10: Math.floor(50 + Math.random() * 40),
      co2: Math.floor(800 + Math.random() * 300)
    };
    
    // Simulate MQTT message
    const message = JSON.stringify(testData);
    console.log('🧪 Simulating MQTT message:', message);
    
    // Trigger message handler
    mqttClient.emit('message', MQTT_TOPIC, Buffer.from(message));
    
    res.json({
      success: true,
      message: 'Test data published',
      data: testData
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Test error',
      message: error.message
    });
  }
});

// ================= START SERVER =================
app.listen(PORT, () => {
  console.log(`\n🚀 Server running on port ${PORT}`);
  console.log(`📡 Web Interface: http://localhost:${PORT}`);
  console.log(`🌐 SSE Endpoint: http://localhost:${PORT}/sse`);
  console.log(`📊 REST API: http://localhost:${PORT}/api`);
  console.log(`🩺 Health Check: http://localhost:${PORT}/health`);
  console.log(`📈 Status: http://localhost:${PORT}/api/status`);
  console.log(`🧪 Test Publish: POST http://localhost:${PORT}/api/test/publish`);
  
  console.log('\n📋 Expected MQTT message format from ESP32:');
  console.log(`  Topic: ${MQTT_TOPIC}`);
  console.log(`  JSON Format: {
    "temp": 25.5,
    "hum": 60.0,
    "pm1": 10,
    "pm25": 20,
    "pm10": 30,
    "co2": 1000
  }`);
});