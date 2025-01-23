
//  g++ misc2.cpp -O3 `PKG_CONFIG_PATH=/usr/local/lib/pkgconfig: pkg-config --libs fmt` -lredis++ -lhiredis -pthread -lstdc++fs -lboost_thread -lboost_log -lboost_log_setup -lpthread 
//
//g++ run_base.cpp  misc2.cpp -lcrossguid -luuid -lfmt -laws-cpp-sdk-s3 -laws-cpp-sdk-core -lredis++ -lhiredis -lparquet -larrow -DBOOST_LOG_DYN_LINK -lboost_thread -lboost_log -lboost_log_setup -lpthread -lSimpleAmqpClient -lboost_thread -lboost_log -lboost_log_setup -lpthread -std=c++2a  -o run_base
#include <iostream>
#include <fstream>
#include <fmt/core.h>
#include <filesystem>
#include <random>
#include <iomanip>
#include <string>
#include <chrono>

#include <crossguid/guid.hpp>
#include <sw/redis++/redis++.h>
#include <SimpleAmqpClient/SimpleAmqpClient.h>


constexpr int FLOAT_MIN = 0;
constexpr int FLOAT_MAX = 1;
constexpr auto EXCHANGE_NAME = "opt4cast_exchange";

std::string get_env_var(std::string const &key, std::string const &default_value) {
        const char *val = std::getenv(key.c_str());
        return val == nullptr ? std::string(default_value) : std::string(val);
}





int main(int argc, char *argv[]) {

     std::string msu_cbpo_path = get_env_var("MSU_CBPO_PATH", "/opt/opt4cast");
    std::string emo_data = argv[1];
    /*
    std::string emo_data = fmt::format("{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}",scenario_name,\
                                      atm_dep_data_set, back_out_scenario, base_condition,\
                                      base_load, cost_profile, climate_change_data_set,\
                                      ncounties, historical_crop_need_scenario, point_source_data_set,\
                                      scenario_type, soil_p_data_set, source_data_revision, geography);
                                      */
    std::string emo_uuid = argv[2];
    int ncounties = std::stoi(argv[3]);
   
    std::string dir_path = fmt::format("{}/output/nsga3/{}", msu_cbpo_path, emo_uuid);
    auto ret = std::filesystem::create_directories(dir_path);

    dir_path = fmt::format("{}/output/nsga3/{}/config", msu_cbpo_path, emo_uuid);
    ret = std::filesystem::create_directories(dir_path);
    if (!ret) {
      std::cout << "create_directories() failed or it is already created\n";
    }

  std::string AMQP_HOST = get_env_var("AMQP_HOST", "localhost");
  std::string AMQP_USERNAME = get_env_var("AMQP_USERNAME", "guest" );
  std::string AMQP_PASSWORD = get_env_var("AMQP_PASSWORD", "guest");
  std::string AMQP_PORT = get_env_var("AMQP_PORT", "5672");



  AmqpClient::Channel::OpenOpts opts;
  opts.host = AMQP_HOST.c_str();
  opts.port = 5672;
  opts.vhost = "/";
  opts.frame_max = 131072;
  opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(AMQP_USERNAME, AMQP_PASSWORD);


  std::string REDIS_HOST = get_env_var("REDIS_HOST", "localhost");
  std::string REDIS_PORT= get_env_var("REDIS_PORT", "6379");
    std::string REDIS_DB_OPT = get_env_var("REDIS_DB_OPT", "1");
    std::string REDIS_URL = fmt::format("tcp://{}:{}/{}", REDIS_HOST, REDIS_PORT, REDIS_DB_OPT);
    auto redis = sw::redis::Redis(REDIS_URL);

  std::random_device rd;
  std::default_random_engine eng(rd());
  std::uniform_real_distribution<float> distr(FLOAT_MIN, FLOAT_MAX);


  int pollutant_id = 0;

  int nvars, nconstraints;
  //add emo_data
  redis.hset("emo_data", emo_uuid, emo_data);

  //emo
  std::string scenario_id = *redis.lpop("scenario_ids");
  redis.hset("emo_to_initialize", emo_uuid, scenario_id); 
    
    auto channel = AmqpClient::Channel::Open(opts);
    //channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, false, true, true);
    auto passive = false; //meaning you want the server to create the exchange if it does not already exist.
    auto durable = true; //meaning the exchange will survive a broker restart
    auto auto_delete = false; //meaning the queue will not be deleted once the channel is closed
    channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, passive, durable, auto_delete);
    // Create the receiver queue
    //
    auto generate_queue_name ="";
    auto exclusive = false; //meaning the queue can be accessed in other channels
    //auto queue_name = channel->DeclareQueue("", false, true, true, true);
    auto queue_name = channel->DeclareQueue(generate_queue_name, passive, durable, exclusive, auto_delete);
    channel->BindQueue(queue_name, EXCHANGE_NAME, emo_uuid);
  
    // Send the message
    try
    {
      auto message = AmqpClient::BasicMessage::Create(emo_uuid);
      auto routing_name = "opt4cast_initialization";
      channel->BasicPublish(EXCHANGE_NAME,  routing_name, message, false, false);
    }
    catch (const std::exception& error)
    {
      std::cout<<error.what()<<std::endl;
      exit(0);
    }
  

    //Receive the message back
    std::cout<<fmt::format("[*] Waiting for server to initialize: {} \n", emo_uuid);

    //auto consumer_tag = channel->BasicConsume(queue_name, "",  true, true, true, 10);
    
    auto no_local = false; 
    auto no_ack = true; //meaning the server will expect an acknowledgement of messages delivered to the consumer
    auto message_prefetch_count = 1;
    auto consumer_tag = channel->BasicConsume(queue_name, generate_queue_name, no_local, no_ack, exclusive, message_prefetch_count);
    auto envelop = channel->BasicConsumeMessage(consumer_tag);
    auto message_payload = envelop->Message()->Body();
    auto routing_key = envelop->RoutingKey();
    channel->BasicCancel(consumer_tag);
    channel->DeleteQueue(queue_name); // 'queue_name' is the name of the queue you want to delete

    return 0;
}


