all : producer consumer
.PHONY:all

consumer : ConsumerMain.cpp KafkaConsumer.h
	g++ -g ConsumerMain.cpp -o consumer -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++ -lpthread -fpermissive

producer : ProducerMain.cpp 
	g++ -g ProducerMain.cpp -o producer -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++