#include <cppkafka/cppkafka.h>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <csignal>
#include <ctime>
#include <chrono>
#include <vector>

using namespace std;
using namespace cppkafka;
using namespace std::chrono;

bool running = true;
int m_number = 0;

uint64_t getMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}
void displayResults (vector <long> res){
    for(auto const& value: res) {
        cout << value <<endl;
    }
    cout<<endl<<"---------------------------"<<endl;
}

int main() {

    string topic = "test";
    vector <long> res;
    string stop = "STOP";


    Configuration config = {
            { "metadata.broker.list", "127.0.0.1:9092"},
            {"fetch.wait.max.ms" , 1},
            {"group.id", topic},
            {"fetch.wait.max.ms", 1},
            {"enable.auto.commit", false},
            {"auto.offset.reset", "latest"}

    };

    Consumer consumer(config);
    consumer.subscribe({topic});

    signal(SIGINT, [](int) { running = false; });

    cout << "Consuming messages from topic " << topic << endl;

    while (running) {
        // Try to consume a message
        //std::vector <float> timers;
        Message msg = consumer.poll();
        if (msg) {
            if(msg.get_payload()==stop){
                displayResults(res);
            }

            if (msg.get_error()) {
                // Ignore EOF notifications from rdkafka
                if (!msg.is_eof()) {
                    cout << "[+] Received error notification: " << msg.get_error() << endl;
                }
            }
            else {
                // Print the key (if any)
                if (msg.get_key()) {
                    cout << msg.get_key() << " -> ";
                }


                //cout <<"Message offset numer : "<< msg.get_offset() <<" payload: " << msg.get_payload() << msg.get_timestamp().get().get_timestamp().count() << endl;
                res.push_back(getMs()-msg.get_timestamp().get().get_timestamp().count());

                // Now commit the message
                consumer.commit(msg);
            }

        }
    }
}