cmake_minimum_required(VERSION 3.16)

project(binance_capture LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -O2)
endif()

find_package(Boost REQUIRED COMPONENTS system thread)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json 3.2 REQUIRED)

include_directories(include)

add_executable(binance_capture src/main.cpp src/binance_capture.cpp src/market_replay.cpp src/rest_snapshot_client.cpp src/metrics.cpp)
target_include_directories(binance_capture PRIVATE include)
target_link_libraries(binance_capture PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json)

add_executable(order_book_tests tests/test_order_book.cpp src/binance_capture.cpp src/market_replay.cpp src/metrics.cpp)
target_include_directories(order_book_tests PRIVATE include)
target_link_libraries(order_book_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json)

add_executable(market_replay_tests tests/test_replay.cpp src/binance_capture.cpp src/market_replay.cpp src/metrics.cpp)
target_include_directories(market_replay_tests PRIVATE include)
target_link_libraries(market_replay_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json)

add_executable(resync_tests tests/test_resync.cpp src/binance_capture.cpp src/market_replay.cpp src/rest_snapshot_client.cpp src/metrics.cpp)
target_include_directories(resync_tests PRIVATE include)
target_link_libraries(resync_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json)

add_executable(metrics_tests tests/test_metrics.cpp src/binance_capture.cpp src/market_replay.cpp src/metrics.cpp)
target_include_directories(metrics_tests PRIVATE include)
target_link_libraries(metrics_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto Boost::system Boost::thread nlohmann_json::nlohmann_json)

if(WIN32)
  target_link_libraries(binance_capture PRIVATE ws2_32)
  target_link_libraries(order_book_tests PRIVATE ws2_32)
  target_link_libraries(market_replay_tests PRIVATE ws2_32)
  target_link_libraries(resync_tests PRIVATE ws2_32)
  target_link_libraries(metrics_tests PRIVATE ws2_32)
endif()

if(MSVC)
  target_compile_options(order_book_tests PRIVATE /UNDEBUG)
  target_compile_options(market_replay_tests PRIVATE /UNDEBUG)
  target_compile_options(resync_tests PRIVATE /UNDEBUG)
  target_compile_options(metrics_tests PRIVATE /UNDEBUG)
else()
  target_compile_options(order_book_tests PRIVATE -UNDEBUG)
  target_compile_options(market_replay_tests PRIVATE -UNDEBUG)
  target_compile_options(resync_tests PRIVATE -UNDEBUG)
  target_compile_options(metrics_tests PRIVATE -UNDEBUG)
endif()

enable_testing()
add_test(NAME order_book_tests COMMAND order_book_tests)
add_test(NAME market_replay_tests COMMAND market_replay_tests)
add_test(NAME resync_tests COMMAND resync_tests)
add_test(NAME metrics_tests COMMAND metrics_tests)
