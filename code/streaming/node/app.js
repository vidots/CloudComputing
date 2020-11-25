var express = require('express')
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var Kafka = require('node-rdkafka')

app.use(express.static('public'))

var writer_topic_times = "stock-analysis_times-topic"
var writer_topic_trend  = "stock-analysis_trend-topic"
var bootstrapServers = 'hadoop-node-1:9092,hadoop-node-2:9092,hadoop-node-3:9092';
var consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': bootstrapServers
})

var port = 3001;

app.get('/', function(req, res){
    res.sendfile('index.html');
});

app.get("/launch", function(req, res) {
    console.log("launch spark task....")
})

io = io.on('connection', function(socket){
    console.log('a user connected');
    socket.on('disconnect', function(){
        console.log('user disconnected');
    });
});

consumer.connect()
consumer.on('ready', function() {
    console.log('kafka ready ...')
    consumer.subscribe([writer_topic_times, writer_topic_trend])
    consumer.consume()
}).on('connection.failure', function(err, metrics) {
    console.log(err)
})
.on('data', function(data) {
    var msg = JSON.parse(data.value.toString());
    if('total' in msg) {
        io.emit("total", msg);
    } else if('trend' in msg) {
        io.emit("trend", msg);
    }
    
})

http.listen(port, function(){
    console.log("服务器运行在端口： " + port)
});
