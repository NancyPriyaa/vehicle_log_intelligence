#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <librdkafka/rdkafka.h>

std::string generate_log() {
    int cpu = rand() % 100;
    int temp = 60 + rand() % 40;

    std::string severity = (cpu > 85 || temp > 90) ? "ERROR" : "INFO";

    std::string json =
        "{"
        "\"vehicle_id\":\"VH_101\","
        "\"module\":\"ADAS\","
        "\"severity\":\"" + severity + "\","
        "\"cpu_usage\":" + std::to_string(cpu) + ","
        "\"temperature\":" + std::to_string(temp) +
        "}";

    return json;
}

int main() {
    srand(time(nullptr));

    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "Error setting Kafka config: " << errstr << std::endl;
        return 1;
    }

    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    if (!producer) {
        std::cerr << "Producer creation failed: " << errstr << std::endl;
        return 1;
    }

    while (true) {
        std::string log = generate_log();

        rd_kafka_producev(
            producer,
            RD_KAFKA_V_TOPIC("vehicle.logs.raw"),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE((void*)log.c_str(), log.size()),
            RD_KAFKA_V_END
        );

        rd_kafka_poll(producer, 0);

        std::cout << "Sent: " << log << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rd_kafka_flush(producer, 5000);
    rd_kafka_destroy(producer);

    return 0;
}
