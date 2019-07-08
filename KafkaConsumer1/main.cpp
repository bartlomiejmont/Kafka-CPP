#include <cppkafka/cppkafka.h>
#include <iostream>
#include <ctime>
#include <string>
#include <chrono>

using namespace std;
using namespace cppkafka;
using namespace std::chrono;

uint64_t getMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}


int main() {
    string topic = "test";

    Configuration config = {
            { "metadata.broker.list", "127.0.0.1:9092" },
            { "group.id", topic },
            // Disable auto commit
            { "enable.auto.commit", false },
            //{"debug", "all"}
    };

    // Construct from some config we've defined somewhere
    Consumer consumer(config);

    consumer.subscribe({ topic });

    cout << "Consuming messages from topic " << topic << endl;

    auto ms = std::chrono::microseconds(10);

// Now loop forever polling for messages
    while (true) {
        // Try to consume a message

        Message msg = consumer.poll();
        if (msg) {
            // If we managed to get a message
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
                // Print the payload
                cout << msg.get_payload()<<" OFSET: "<< msg.get_offset() <<" TIME: "<<getMs() - msg.get_timestamp().get().get_timestamp().count()<< endl;
                // Now commit the message
                cout.flush();
                consumer.commit(msg);
            }
        }
    }

}

//msg.get_timestamp().get().get_timestamp().count()