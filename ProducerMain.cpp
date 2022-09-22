
#include "KafkaProducer.h"
#include <string>
using namespace std;

int main(int argc, char const *argv[])
{
    KafkaProducer producer;
    producer.Init("localhost", 9092, true, 1000);
    int cnt = 0;
    while (cnt < 10)
    {
        if (producer.IsGood())
        {
            std::string topic = "heima";
            std::string message = "I love you " + to_string(cnt);
            producer.Send(topic, message, 1000);
        }
        cnt++;
    }
    
    
    return 0;
}

// g++ -o producer KafkaProducer.h ProducerMain.cpp -I/usr/local/include/librdkafka/ -L/usr/local/include/ -lrdkafka -lrdkafka++