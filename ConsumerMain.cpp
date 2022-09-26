
#include "KafkaConsumer.h"
#include <csignal>

static bool run = true;
void sigterm(int sig)
{
    run = false;
}

int main(int argc, char const *argv[])
{
    KafkaConsumer consumer;
    //std::string topic = "heima";
    std::vector<std::string> topics = {"heima","test02"};
    consumer.SetTopic(topics);    
    consumer.Init("127.0.0.1", 9092, "101",5);
    consumer.Consume();
    if(run == false)
    {
        std::cout << "stop consumer" << std::endl;
        consumer.Stop();
    }
    while(getchar() != '\n')
    return 0;
}

