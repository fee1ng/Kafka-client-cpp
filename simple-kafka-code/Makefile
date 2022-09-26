all : ws_client consumer
.PHONY:all

consumer : consumer.cpp
	g++ consumer.cpp -o consumer  -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++ -I/usr/local/src/zabbix_data_moniotr_system/include -L/usr/local/src/zabbix_data_moniotr_system/lib -lpthread -lglog

ws_client : ws_client.cpp producer.o
	c++  -std=c++11 ws_client.cpp -o run -I/usr/local/src/zabbix_data_moniotr_system/include -L/usr/local/src/zabbix_data_moniotr_system/lib -lhv -lpthread -fpermissive -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++

producer.o : producer.cpp producer.h
	g++ -c producer.cpp -o producer  -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++ -lpthread
