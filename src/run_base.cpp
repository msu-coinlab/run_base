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


#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

#include "misc.hpp"

constexpr int FLOAT_MIN = 0;
constexpr int FLOAT_MAX = 1;
constexpr auto EXCHANGE_NAME = "opt4cast_exchange";

void split_str( std::string const &str, const char delim,
        std::vector <std::string> &out ) {
  std::stringstream s(str);

  std::string s2;

  while (std:: getline (s, s2, delim) ) {
      out.push_back(s2); // store the string in s
  }
}

long get_time(){
  namespace sc = std::chrono;
  auto time = sc::system_clock::now();

  auto since_epoch = time.time_since_epoch();
  auto millis = sc::duration_cast<sc::milliseconds>(since_epoch);

  long now = millis.count(); // just like java (new Date()).getTime();
  return now;
}

void init_logging(std::string log_prefix_file_name, std::string source)
{

  std::string msu_cbpo_path = getEnvVar("MSU_CBPO_PATH", "/opt/opt4cast");
  boost::log::register_simple_formatter_factory<boost::log::trivial::severity_level, char>("Severity");

  boost::log::add_file_log(
        boost::log::keywords::auto_flush          = true,
        boost::log::keywords::target              = "log",        
        //boost::log::keywords::file_name           = fmt::format("{}/log/{}_{}_%Y%m%d.log",msu_cbpo_path, log_prefix_file_name, source),
        boost::log::keywords::file_name           = fmt::format("{}/log/{}_{}.log",msu_cbpo_path, log_prefix_file_name, source),
        boost::log::keywords::open_mode           = std::ios::out | std::ios::app,
        boost::log::keywords::time_based_rotation = boost::log::sinks::file::rotation_at_time_point(0, 0, 0),

        //boost::log::keywords::file_name = log_file_name,
        boost::log::keywords::rotation_size = 10 * 1024 * 1024,
        boost::log::keywords::format = "[%TimeStamp%] [%ThreadID%] [%Severity%] [%ProcessID%] [%LineID%] [%MyAttr%] %Message%"
    );
  boost::log::core::get()->set_filter
    (
        boost::log::trivial::severity >= boost::log::trivial::info
    );

  boost::log::core::get()->add_global_attribute("MyAttr", boost::log::attributes::constant<std::string>(source));
  boost::log::add_common_attributes();
}


void generate_nsga3_input(std::string input_filename, int nvars, int nconstraints){
    int nobjs = 2;
    int pop_size = 100;
    int n_iters = 100;
    int n_steps_reference_points = 60;
    int adaptive_ref_points = 1;
    double prob_xover = 1.0;

    double xover_dist_idx = 30;
    double mut_dist_idx = 20;
    int n_bin_vars = 0;
    int gnuplot = 1;
    int gnuplot_x = 1;
    int gnuplot_y = 2;


    std::ofstream fout(input_filename);
    fout<<nobjs<<"\n";
    fout<<n_steps_reference_points<<"\n";
    fout<<adaptive_ref_points<<"\n";
    fout<<pop_size<<"\n";
    fout<<n_iters<<"\n";
    fout<<nconstraints<<"\n";
    fout<<nvars<<"\n";

    //for (int i = 0; i < nvars; i++)
    //{
    //    fout<<0<<" "<<1<<"\n";
    //}
    
    fout<<prob_xover<<"\n";
    fout<<xover_dist_idx<<"\n";
    fout<<mut_dist_idx<<"\n";
    fout<<n_bin_vars<<"\n";
    fout<<gnuplot<<"\n";
    fout<<gnuplot_x<<"\n";
    fout<<gnuplot_y<<"\n";
    fout.close();
}

int main(int argc, char *argv[]) {

     std::string msu_cbpo_path = getEnvVar("MSU_CBPO_PATH", "/opt/opt4cast");

    int atm_dep_data_set = std::stoi(argv[1]);
    int back_out_scenario = std::stoi(argv[2]);
    int base_condition = std::stoi(argv[3]);
    int base_load = std::stoi(argv[4]);
    int cost_profile = std::stoi(argv[5]);
    int climate_change_data_set = std::stoi(argv[6]);
    int historical_crop_need_scenario = std::stoi(argv[7]);
    int point_source_data_set = std::stoi(argv[8]);
    int scenario_type = std::stoi(argv[9]);
    int soil_p_data_set = std::stoi(argv[10]);
    int source_data_revision = std::stoi(argv[11]);
    std::string emo_uuid = argv[12];

    int ncounties = std::stoi(argv[13]);
    std::string geography = argv[14];
    for(int i(0); i< ncounties-1; i++) {
      geography = fmt::format("{}_{}", geography, argv[15+i]);
    }
    //auto emo_uuid = xg::newGuid().str();
   
    std::string dir_path = fmt::format("{}/output/nsga3/{}", msu_cbpo_path, emo_uuid);
    auto ret = std::filesystem::create_directories(dir_path);

    dir_path = fmt::format("{}/output/nsga3/{}/config", msu_cbpo_path, emo_uuid);
    ret = std::filesystem::create_directories(dir_path);
    if (!ret) {
      std::cout << "create_directories() failed or it is already created\n";
    }
    std::string scenario_name = "tmpscenarioname";
    std::string emo_data = fmt::format("{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}",scenario_name,\
                                      atm_dep_data_set, back_out_scenario, base_condition,\
                                      base_load, cost_profile, climate_change_data_set,\
                                      ncounties, historical_crop_need_scenario, point_source_data_set,\
                                      scenario_type, soil_p_data_set, source_data_revision, geography);

    std::cout<<emo_data<<std::endl;
  std::string AMQP_HOST = getEnvVar("AMQP_HOST");
  std::string AMQP_USERNAME = getEnvVar("AMQP_USERNAME");
  std::string AMQP_PASSWORD = getEnvVar("AMQP_PASSWORD");
  std::string AMQP_PORT = getEnvVar("AMQP_PORT");

  std::vector<std::string> uuid_vec;
  init_logging("file2", "NSGA-III");

  AmqpClient::Channel::OpenOpts opts;
  opts.host = AMQP_HOST.c_str();
  opts.port = 5672;
  opts.vhost = "/";
  opts.frame_max = 131072;
  opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(AMQP_USERNAME, AMQP_PASSWORD);


  std::string REDIS_HOST = getEnvVar("REDIS_HOST");
  std::string REDIS_PORT= getEnvVar("REDIS_PORT");
  auto redis = sw::redis::Redis(fmt::format("tcp://{}:{}/1", REDIS_HOST, REDIS_PORT));

  std::random_device rd;
  std::default_random_engine eng(rd());
  std::uniform_real_distribution<float> distr(FLOAT_MIN, FLOAT_MAX);

     /*
    if (argc < 1) {
        std::cerr<<"generate_input prefix_file\n";
        exit(1);
    }
    */

    //std::string prefix_file = argv[1];

  int pollutant_id = 0;

  int nvars, nconstraints;
  //add emo_data
  redis.hset("emo_data", emo_uuid, emo_data);

  //emo
  std::string scenario_id = *redis.lpop("scenario_ids");

  redis.hset("emo_to_initialize", emo_uuid, scenario_id); 
  
  try
  {
    auto channel = AmqpClient::Channel::Open(opts);
    //channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, false, true, false);
    auto passive = false; //meaning you want the server to create the exchange if it does not already exist.
    auto durable = true; //meaning the exchange will survive a broker restart
    auto auto_delete = false; //meaning the queue will not be deleted once the channel is closed
    channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, passive, durable, auto_delete);
    auto message = AmqpClient::BasicMessage::Create(emo_uuid);
    auto routing_name = "opt4cast_initialization";
    channel->BasicPublish(EXCHANGE_NAME,  routing_name, message, false, false);
    //std::clog<< " [x] Sent "<<severity<<":" << msg_str << "\n";
    //BOOST_LOG_TRIVIAL(info) << " [x] Sent "<<severity<<":" << msg_str << "\n";
  }
  catch (const std::exception& error)
  {
    std::cout<<error.what()<<std::endl;
    BOOST_LOG_TRIVIAL(error) << error.what();
    exit(0);
  }

  std::cout<<"emo_uuid: "<<emo_uuid<<std::endl;
    auto channel = AmqpClient::Channel::Open(opts);

    //channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, false, true, true);

    auto passive = false; //meaning you want the server to create the exchange if it does not already exist.
    auto durable = true; //meaning the exchange will survive a broker restart
    auto auto_delete = false; //meaning the queue will not be deleted once the channel is closed
    channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, passive, durable, auto_delete);
    //auto queue_name = channel->DeclareQueue("", false, true, true, true);
    auto generate_queue_name ="";
    auto exclusive = false; //meaning the queue can be accessed in other channels
    //auto queue_name = channel->DeclareQueue("", false, true, true, true);
    auto queue_name = channel->DeclareQueue(generate_queue_name, passive, durable, exclusive, auto_delete);

    //std::clog << "Queue with name '" << queue_name << "' has been declared.\n";
    BOOST_LOG_TRIVIAL(info) << "Queue with name '" << queue_name << "' has been declared.";
    channel->BindQueue(queue_name, EXCHANGE_NAME, emo_uuid);

    std::cout<<fmt::format("[*] Waiting for server to initialize: {} \n", emo_uuid);

    auto consumer_tag = channel->BasicConsume(queue_name, "",  true, true, true, 10);
    //"./"+prefix_init_file+"/"+prefix_init_file+"_vars.txt";
    //std::clog

    BOOST_LOG_TRIVIAL(info) << fmt::format("Consumer tag {} for emo_uuid: {}", consumer_tag, emo_uuid);
    auto envelop = channel->BasicConsumeMessage(consumer_tag);
    auto message_payload = envelop->Message()->Body();
    auto routing_key = envelop->RoutingKey();

    
    std::string filename = fmt::format("execution_log.csv");
    std::ofstream execution_log_file(filename);
  

    if(true && routing_key == emo_uuid){
      BOOST_LOG_TRIVIAL(info) << "Initializing scenario: " << message_payload;
      read_global_files(emo_uuid.c_str(), pollutant_id, cost_profile);
      get_info(nvars, nconstraints, fmt::format("{}/output/nsga3/{}/config/", msu_cbpo_path, emo_uuid));
      std::clog<<"nvars: "<<nvars<<" nconstraints: "<<nconstraints<<std::endl;
      //load_info(fmt::format("{}/output/nsga3/{}/config/", msu_cbpo_path,emo_uuid), emo_uuid);

      //std::string input_filename  = fmt::format("{}/alg/nsga34cast/input_data/{}_cbw.in", msu_cbpo_path, emo_uuid);
      std::string input_filename  = fmt::format("{}/output/nsga3/{}/config/{}_cbw.in", msu_cbpo_path, emo_uuid, emo_uuid);

      generate_nsga3_input(input_filename, nvars, nconstraints);
    }
    else {
        BOOST_LOG_TRIVIAL(error) << fmt::format("Error at receiving message with routing_key {} and exec_uuid {}", emo_uuid, message_payload);
        std::clog<<"Error en receiving scenarios\n";
    }
    

    return 0;
}


